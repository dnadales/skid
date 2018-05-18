{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric      #-}

-- | This module provides function for running a node of the key-value store
-- network.
module Database.SKID.Node
    (runNode)
where

import           Control.Concurrent                                 (threadDelay)
import           Control.Concurrent.Async                           (Async,
                                                                     async)
import           Control.Distributed.Process                        (Process,
                                                                     ProcessId,
                                                                     ProcessMonitorNotification (ProcessMonitorNotification),
                                                                     WhereIsReply (WhereIsReply),
                                                                     expectTimeout,
                                                                     getSelfNode,
                                                                     getSelfPid,
                                                                     match,
                                                                     matchAny,
                                                                     monitor,
                                                                     receiveWait,
                                                                     register,
                                                                     say, send,
                                                                     spawnLocal,
                                                                     whereisRemoteAsync)
import           Control.Distributed.Process.Backend.SimpleLocalnet (findPeers, initializeBackend,
                                                                     newLocalNode)
import           Control.Distributed.Process.Node                   (initRemoteTable,
                                                                     runProcess)
import           Control.Monad                                      (forever,
                                                                     unless,
                                                                     void, when)
import           Control.Monad.IO.Class                             (liftIO)
import           Data.Binary                                        (Binary)
import           Data.Foldable                                      (traverse_)
import           Data.Map.Strict                                    (Map)
import qualified Data.Set                                           as Set
import           Data.Typeable                                      (Typeable)
import           GHC.Generics                                       (Generic)
import           Network.Socket                                     (HostName,
                                                                     ServiceName)
import           System.Random                                      (randomRIO)

import           Database.SKID.Node.State                           (Key, PutType (Remote),
                                                                     State,
                                                                     Value,
                                                                     addPeer,
                                                                     getNextKV,
                                                                     getPeers,
                                                                     isBootstrapped,
                                                                     localMap,
                                                                     markBootrsapped,
                                                                     mergeKVs,
                                                                     put,
                                                                     removePeer)

-- | Start a node asynchronously.
runNode :: HostName -> ServiceName -> State -> IO (Async ())
runNode host port st = async $ do
    backend <- initializeBackend host port initRemoteTable
    node    <- newLocalNode backend
    runProcess node $ do
        myNId <- getSelfNode
        -- Find the peer nodes.
        peers <- fmap (filter (/= myNId)) $ liftIO $ findPeers backend 1000000
        -- Register this process so that peers can find it.
        myPId <- getSelfPid
        register skidProc myPId
        -- Ask to the peers where the other skid processes are.
        traverse_ (`whereisRemoteAsync` skidProc) peers
        -- Spawn the process that updates the other nodes upon 'PUT' commands
        void $ spawnLocal updater
        -- Spawn the bootstrapper process.
        void $ spawnLocal (bootstrapper 5)
        -- Keep on waiting for the messages that are sent to this process.
        forever $ receiveWait [ match handleWhereIsReply
                              , match handleHello
                              , match handleMonitorNotification
                              , match handleUpdate
                              , match handleBootstrapRq
                              , matchAny $ \msg -> say $
                                  "Message not handled: " ++ show msg
                              ]

    where
      -- | Service name for the skid-process within this node. This is the name
      -- used to identify peers by means of @whereisRemoteAsync@.
      skidProc :: String
      skidProc = "skid-proc"

      -- | Add a the newly discovered process-id to the state upon arrival of a
      -- @WhereIsReply@ message.
      --
      -- @WhereIsReply@ messages will come after the node starts. Each of the
      -- existing nodes in the network is expected to send this message when
      -- the @whereisRemoteAsync@ message is sent.
      handleWhereIsReply :: WhereIsReply -> Process ()
      handleWhereIsReply (WhereIsReply _ Nothing)    = return ()
      handleWhereIsReply (WhereIsReply _ (Just pId)) = do
          addPeerAndMonitor pId
          myPId <- getSelfPid
          send pId (Hello myPId)

      -- | Add a peer process-id to the state and monitor it.
      addPeerAndMonitor :: ProcessId -> Process ()
      addPeerAndMonitor pId = monitor pId >> addPeer st pId

      -- | Handle the message sent by a newly created skid-process.
      handleHello :: Hello -> Process ()
      handleHello (Hello pId) = addPeerAndMonitor pId

      -- | Handle the message that is generated when a peer process dies.
      handleMonitorNotification :: ProcessMonitorNotification -> Process ()
      handleMonitorNotification (ProcessMonitorNotification _ pId _) =
          removePeer st pId

      -- | Handle updates in the key-value map originated by a peer.
      handleUpdate :: Update -> Process ()
      handleUpdate (Update k v) = put st Remote k v

      -- | Process that listens on the queue of key-values, and send updates as
      -- new values are put there.
      updater :: Process ()
      updater = forever $ do
          (k, v) <- getNextKV st
          peers  <- getPeers st
          traverse_ (`send` Update k v) peers


      -- | Process that handles the bootstrap by sending a bootstrapping
      -- request to the peers (if any). It can be that no peers are discovered
      -- initially if multiple nodes are created at the same time, therefore
      -- the bootstrapping process tries to find peers 'n' times before giving
      -- up.
      bootstrapper :: Int -> Process ()
      bootstrapper n
          | n == 0 = markBootrsapped st
          | otherwise = do
                b <- isBootstrapped st
                unless (0 < n && b) $ do
                    peers <- Set.toList <$> getPeers st
                    when (0 < length peers) $ do
                        -- Select an arbitrary peer.
                        i <- liftIO $ randomRIO (0, length peers - 1)
                        myPId <- getSelfPid
                        send (peers !! i) (BootstrapRq myPId)
                        mBootstrap <- expectTimeout 1000000
                        traverse_ handleBootstrap mBootstrap
                    liftIO $ threadDelay 1000000
                    bootstrapper (n - 1)

      handleBootstrap :: Bootstrap -> Process ()
      handleBootstrap (Bootstrap mm) = do
          mergeKVs st mm
          markBootrsapped st

      handleBootstrapRq :: BootstrapRq -> Process ()
      handleBootstrapRq (BootstrapRq pId) = do
          mm <- localMap st
          send pId (Bootstrap mm)


-- | Message that skid-processes send when they boot up.
newtype Hello = Hello ProcessId
    deriving (Typeable, Generic)

instance Binary Hello

-- | Message that signals a new PUT operation took place at a node.
data Update = Update Key Value
    deriving (Typeable, Generic)

instance Binary Update

-- | Request of the key-value map by the given process-id.
newtype BootstrapRq = BootstrapRq ProcessId
    deriving (Typeable, Generic)

instance Binary BootstrapRq

-- | Message use to bootstrap a node with the existing map.
newtype Bootstrap = Bootstrap (Map Key Value)
    deriving (Typeable, Generic)

instance Binary Bootstrap
