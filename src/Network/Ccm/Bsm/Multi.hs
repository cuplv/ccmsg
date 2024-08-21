module Network.Ccm.Bsm.Multi where

import Network.Ccm.Bsm.Internal
import Network.Ccm.Lens

import Control.Concurrent (forkIO,threadDelay)
import Control.Concurrent.STM
import Data.ByteString (ByteString)
import Data.Map (Map)
import qualified Data.Map as Map
import Data.Set (Set)
import qualified Data.Set as Set
import Data.Foldable (traverse_)
import qualified Data.Store as Store
import Data.Word (Word32)
import Network.Ccm.TCP
import Network.Ccm.Types

getBsmSelfId :: BsmMulti -> NodeId
getBsmSelfId = view bsmSelf

-- getFromInbox :: BsmMulti -> STM (NodeId, ByteString)
-- getFromInbox sock = readBsmInbox (bsmInbox sock)


-- checkAllSent :: BsmMulti -> STM Bool
-- checkAllSent bsm =
--   let f (_,ob,_) = emptyOutbox ob
--   in and <$> traverse f (Map.elems $ bsmTargets bsm)

-- putIntoOutbox :: BsmMulti -> NodeId -> ByteString -> STM ()
-- putIntoOutbox sock n bs = case Map.lookup n (sock^.bsmPeers) of
--   Just Peer (_,q,_) -> writeBsmOutbox q bs
--   Nothing -> error $ "Called putIntoOutbox for unknown: " ++ show n

openBsmServer :: MyAddr -> BsmMulti -> IO ()
openBsmServer selfAddr bsm = do
  forkIO $ runServer selfAddr bsm
  return ()

-- | First retry delay for connection, in microseconds
initialDelay :: Int
initialDelay = 1000 -- 1 ms

openBsmClients :: BsmMulti -> Map NodeId MyAddr -> IO ()
openBsmClients bsm addrs = do
  let
    f targetNode =
      if (bsm^.bsmSelf) > targetNode
      then do
        let addr = case Map.lookup targetNode addrs of
              Just a -> a
              Nothing -> error $ "Cannot open client, no addr for: " ++ show targetNode
        debug (bsm^.bsmDbg) $ "Running client for " ++ show targetNode
        forkIO (runClient initialDelay bsm targetNode addr)
        return ()
      else return ()
  traverse_ f (Set.toList (getPeerIds bsm))
  return ()

runBsmMulti :: Debugger -> NodeId -> Map NodeId MyAddr -> IO (BsmMulti)
runBsmMulti d self addrs = do
  let selfAddr = case Map.lookup self addrs of
        Just addr -> addr
        Nothing -> error "No addr for self"
  bsm <- initBsmMulti d self (Map.keysSet addrs)
  openBsmServer selfAddr bsm
  openBsmClients bsm addrs
  return bsm

testNetwork :: Map NodeId MyAddr
testNetwork = Map.fromList
  [(NodeId 0,MyAddr "127.0.0.1" "8050")
  ,(NodeId 1,MyAddr "127.0.0.1" "8051")
  ,(NodeId 2,MyAddr "127.0.0.1" "8052")
  ,(NodeId 3,MyAddr "127.0.0.1" "8053")
  ]

-- testBsmMulti :: Word32 -> [Word32] -> IO ()
-- testBsmMulti selfN otherN = do
--   dbg <- runQD
--   testBsmMulti' dbg selfN otherN

-- testBsmMulti' :: Debugger -> Word32 -> [Word32] -> IO ()
-- testBsmMulti' dbg selfN otherN = do
--   let self = NodeId selfN
--       others = map NodeId otherN
--   let waitForReady bsm = atomically $ do
--         status <- allReady bsm
--         if status
--           then return ()
--           else retry
--   let recvLoop bsm = do
--         (sender,msg) <- atomically $ getFromInbox bsm
--         debug dbg $ "From " ++ show sender ++ ": " ++ Store.decodeEx msg
--         recvLoop bsm
--   let sendLoop bsm n = do
--         traverse_
--           (\other -> atomically $ putIntoOutbox bsm other (Store.encode $ "Hello #" ++ show n))
--           others
--         debug dbg $ "Sent messages to " ++ show others
--         threadDelay 1000000
--         sendLoop bsm (n + 1)
--   bsm <- runBsmMulti dbg self testNetwork
--   debug dbg $ "Waiting for all nodes to be ready..."
--   waitForReady bsm
--   forkIO $ sendLoop bsm 1
--   recvLoop bsm

-- test4 :: IO ()
-- test4 = do
--   d <- runQD
--   forkIO $ testBsmMulti' d 0 [1,2,3]
--   forkIO $ testBsmMulti' d 1 [0,2,3]
--   forkIO $ testBsmMulti' d 2 [0,1,3]
--   testBsmMulti' d 3 [0,1,2]
