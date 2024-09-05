{-# LANGUAGE TemplateHaskell #-}

module Network.Ccm.Bsm.Internal where

import Network.Ccm.Lens
import Network.Ccm.Types

import Control.Concurrent.STM
import Data.ByteString (ByteString)
import Data.Foldable (traverse_)
import Data.Map (Map)
import qualified Data.Map as Map
import Data.Set (Set)
import qualified Data.Set as Set
import Numeric.Natural (Natural)

bsmBoxBound :: Natural
bsmBoxBound = 500000

data PeerMessage
  = PMData ByteString
  deriving (Show,Eq,Ord)

data PeerStatus
  = PSConnected
  | PSDisconnected
  deriving (Show,Eq,Ord)

data Peer
  = Peer
    { _peerOutbox :: TBQueue PeerMessage
    , _peerShutdown :: TMVar ()
    , _peerStatus :: TVar PeerStatus
    }

makeLenses ''Peer

data Bsm
  = Bsm
    { _bsmInbox :: TBQueue (NodeId, ByteString)
    , _bsmPeers :: Map NodeId Peer
    , _bsmSelf :: NodeId
    }

makeLenses ''Bsm

isEmptyInbox :: Bsm -> STM Bool
isEmptyInbox bsm = isEmptyTBQueue (bsm ^. bsmInbox)

getPeerIds :: Bsm -> Set NodeId
getPeerIds bsm = Map.keysSet (bsm ^. bsmPeers)

getNotReady :: Bsm -> STM [NodeId]
getNotReady bsm =
  (map fst . filter f2)
  <$> (traverse f . Map.toList $ bsm^.bsmPeers)

  where f (n,p) = do
          status <- readTVar (p^.peerStatus)
          return (n,status)
        f2 (n,status) = not (status == PSConnected)

allReady :: Bsm -> STM Bool
allReady bsm = null <$> getNotReady bsm

readInbox :: Bsm -> STM [(NodeId, ByteString)]
readInbox bsm = do
  e <- isEmptyInbox bsm
  if e
    then tryReadInbox bsm
    else retry

tryReadInbox :: Bsm -> STM [(NodeId, ByteString)]
tryReadInbox bsm = flushTBQueue (bsm^.bsmInbox)

sendBsm :: Bsm -> SendTarget -> ByteString -> STM ()
sendBsm bsm t bs =
  let ids = case t of
              SendTo ids -> ids
              SendAll -> getPeerIds bsm
  in traverse_ (\n -> sendPeer bsm n bs) ids

sendPeer :: Bsm -> NodeId -> ByteString -> STM ()
sendPeer bsm n bs = case Map.lookup n (bsm^.bsmPeers) of
  Just p -> writeTBQueue (p^.peerOutbox) (PMData bs)
  Nothing -> error $ "Called sendPeer for unknown peer: " ++ show n

shutdownBsm :: Bsm -> STM ()
shutdownBsm bsm = traverse_ (shutdownPeer bsm) (getPeerIds bsm)

shutdownPeer :: Bsm -> NodeId -> STM ()
shutdownPeer bsm n = case Map.lookup n (bsm^.bsmPeers) of
  Just p -> tryPutTMVar (p^.peerShutdown) () >> return ()
  Nothing -> error $ "Called shutdownPeer for unknown peer: " ++ show n

initBsm :: NodeId -> Set NodeId -> IO (Bsm)
initBsm self ns = do
  let mkPeer n = do
        status <- newTVarIO PSDisconnected
        sd <- newEmptyTMVarIO
        ob <- newTBQueueIO bsmBoxBound
        let peer = Peer
              { _peerOutbox = ob
              , _peerShutdown = sd
              , _peerStatus = status
              }
        return (n,peer)
  inbox <- newTBQueueIO bsmBoxBound
  peers <- Map.fromList
    <$> mapM mkPeer (Set.toList (Set.delete self ns))
  return $ Bsm
    { _bsmInbox = inbox
    , _bsmPeers = peers
    , _bsmSelf = self
    }
