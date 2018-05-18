-- | This module provides function for running a node of the key-value store
-- network.
module Database.SKID.Node
    (runNode)
where

import           Control.Concurrent.Async                           (Async,
                                                                     async)
import           Control.Distributed.Process                        (Process, WhereIsReply (WhereIsReply),
                                                                     getSelfNode,
                                                                     getSelfPid,
                                                                     match,
                                                                     matchAny,
                                                                     receiveWait,
                                                                     register,
                                                                     say,
                                                                     whereisRemoteAsync)
import           Control.Distributed.Process.Backend.SimpleLocalnet (findPeers, initializeBackend,
                                                                     newLocalNode)
import           Control.Distributed.Process.Node                   (initRemoteTable,
                                                                     runProcess)
import           Control.Monad                                      (forever)
import           Control.Monad.IO.Class                             (liftIO)
import           Data.Foldable                                      (traverse_)
import           Network.Socket                                     (HostName,
                                                                     ServiceName)

import           Database.SKID.Node.State                           (State,
                                                                     addPeer)


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
        -- Ask to the peers where the other skid processes arep.
        traverse_ (`whereisRemoteAsync` skidProc) peers
        forever $ receiveWait [ match handleWhereIsReply
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
      handleWhereIsReply (WhereIsReply _ (Just pId)) = addPeer st pId
