{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric      #-}

-- | This module provides function for running a node of the key-value store
-- network.
module Database.SKID.Node
    (runNode)
where

import           Control.Concurrent.Async                           (Async,
                                                                     async)
import           Control.Distributed.Process                        (Process,
                                                                     ProcessId,
                                                                     ProcessMonitorNotification (ProcessMonitorNotification),
                                                                     WhereIsReply (WhereIsReply),
                                                                     getSelfNode,
                                                                     getSelfPid,
                                                                     match,
                                                                     matchAny,
                                                                     monitor,
                                                                     receiveWait,
                                                                     register,
                                                                     say, send,
                                                                     whereisRemoteAsync)
import           Control.Distributed.Process.Backend.SimpleLocalnet (findPeers, initializeBackend,
                                                                     newLocalNode)
import           Control.Distributed.Process.Node                   (initRemoteTable,
                                                                     runProcess)
import           Control.Monad                                      (forever)
import           Control.Monad.IO.Class                             (liftIO)
import           Data.Binary                                        (Binary)
import           Data.Foldable                                      (traverse_)
import           Data.Typeable                                      (Typeable)
import           GHC.Generics                                       (Generic)
import           Network.Socket                                     (HostName,
                                                                     ServiceName)

import           Database.SKID.Node.State                           (State,
                                                                     addPeer,
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
        -- Keep on waiting for the messages that are sent to this process.
        forever $ receiveWait [ match handleWhereIsReply
                              , match handleHello
                              , match handleMonitorNotification
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

-- | Message that skid-processes send when they boot up.
data Hello = Hello ProcessId
    deriving (Typeable, Generic)

instance Binary Hello
