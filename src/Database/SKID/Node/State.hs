-- | State of a node, plus operations on this state. The state of a node
--  includes the in-memory key-value map, as well as information about its
--  peers.

module Database.SKID.Node.State
    ( State
    , mkState
    , getPeers
    , addPeer
    , removePeer
    )
where

import           Control.Concurrent.STM      (atomically)
import           Control.Concurrent.STM.TVar (TVar, modifyTVar', newTVarIO,
                                              readTVarIO)
import           Control.Distributed.Process (ProcessId)
import           Control.Monad.IO.Class      (MonadIO, liftIO)
import           Data.Set                    (Set)
import qualified Data.Set                    as Set

-- | State of a node.
data State = State
    {  -- | Set of peers known so far.
       peers :: TVar (Set ProcessId)
    }

mkState :: IO State
mkState = State <$> newTVarIO (Set.empty)

-- | Get set of the peers known so far.
getPeers :: MonadIO m => State -> m (Set ProcessId)
getPeers st = liftIO $
    readTVarIO (peers st)

-- | Add a peer to the set of known peers.
addPeer :: MonadIO m => State -> ProcessId -> m ()
addPeer st pId = liftIO $ atomically $
    modifyTVar' (peers st) (pId `Set.insert`)

-- | Remove a peer from the set of known peers.
removePeer :: MonadIO m => State -> ProcessId -> m ()
removePeer st pId = liftIO $ atomically $
    modifyTVar' (peers st) (pId `Set.delete`)
