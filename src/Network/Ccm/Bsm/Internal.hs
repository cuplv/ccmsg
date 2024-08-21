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

data SendTarget
  = SendTo (Set NodeId)
  | SendAll
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

data BsmMulti
  = BsmMulti { _bsmDbg :: Debugger
             , _bsmInbox :: TBQueue (NodeId, ByteString)
             , _bsmPeers :: Map NodeId Peer
             , _bsmSelf :: NodeId
             }

makeLenses ''BsmMulti

isEmptyInbox :: BsmMulti -> STM Bool
isEmptyInbox bsm = isEmptyTBQueue (bsm ^. bsmInbox)

getPeerIds :: BsmMulti -> Set NodeId
getPeerIds bsm = Map.keysSet (bsm ^. bsmPeers)

getNotReady :: BsmMulti -> STM [NodeId]
getNotReady bsm =
  (map fst . filter f2)
  <$> (traverse f . Map.toList $ bsm^.bsmPeers)

  where f (n,p) = do
          status <- readTVar (p^.peerStatus)
          return (n,status)
        f2 (n,status) = not (status == PSConnected)

allReady :: BsmMulti -> STM Bool
allReady bsm = null <$> getNotReady bsm

readInbox :: BsmMulti -> STM [(NodeId, ByteString)]
readInbox bsm = do
  e <- isEmptyInbox bsm
  if e
    then tryReadInbox bsm
    else retry

tryReadInbox :: BsmMulti -> STM [(NodeId, ByteString)]
tryReadInbox bsm = flushTBQueue (bsm^.bsmInbox)

sendBsm :: BsmMulti -> SendTarget -> ByteString -> STM ()
sendBsm bsm t bs =
  let ids = case t of
              SendTo ids -> ids
              SendAll -> getPeerIds bsm
  in traverse_ (\n -> sendPeer bsm n bs) ids

sendPeer :: BsmMulti -> NodeId -> ByteString -> STM ()
sendPeer bsm n bs = case Map.lookup n (bsm^.bsmPeers) of
  Just p -> writeTBQueue (p^.peerOutbox) (PMData bs)
  Nothing -> error $ "Called sendPeer for unknown peer: " ++ show n

shutdownPeer :: BsmMulti -> NodeId -> STM ()
shutdownPeer bsm n = case Map.lookup n (bsm^.bsmPeers) of
  Just p -> tryPutTMVar (p^.peerShutdown) () >> return ()
  Nothing -> error $ "Called shutdownPeer for unknown peer: " ++ show n

initBsmMulti :: Debugger -> NodeId -> Set NodeId -> IO (BsmMulti)
initBsmMulti d self ns = do
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
  return $ BsmMulti
    { _bsmDbg = d
    , _bsmInbox = inbox
    , _bsmPeers = peers
    , _bsmSelf = self
    }
