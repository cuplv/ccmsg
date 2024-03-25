module Network.Ccm.Bsm.Multi where

import Network.Ccm.Bsm.Inbox
import Network.Ccm.Bsm.Internal
import Network.Ccm.Bsm.Outbox

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

boxBound = 1000

data SendTarget
  = SendTo (Set NodeId)
  | SendAll

data BsmMulti
  = BsmMulti { bsmDbg :: Debugger
             , bsmInbox :: BsmInbox
             , bsmTargets :: Map NodeId (MyAddr, BsmOutbox, TVar Bool)
             , bsmSelf :: NodeId
             }

getRemoteNodeIds :: BsmMulti -> Set NodeId
getRemoteNodeIds bsm = Set.delete (bsmSelf bsm) $ Map.keysSet (bsmTargets bsm)

getNotReady :: BsmMulti -> STM [NodeId]
getNotReady bsm =
  (map fst . filter f2)
  <$> (traverse f . Map.toList $ bsmTargets bsm)

  where f (n,(_,_,tv)) = do
          status <- readTVar tv
          return (n,status)
        f2 (n,status) = not status

allReady :: BsmMulti -> STM Bool
allReady bsm = null <$> getNotReady bsm

getBsmSelfId :: BsmMulti -> NodeId
getBsmSelfId = bsmSelf

getFromInbox :: BsmMulti -> STM (NodeId, ByteString)
getFromInbox sock = readBsmInbox (bsmInbox sock)

getManyFromInbox :: BsmMulti -> STM [(NodeId, ByteString)]
getManyFromInbox sock = readManyBsmInbox (bsmInbox sock)

sendBsm :: BsmMulti -> SendTarget -> ByteString -> STM ()
sendBsm bsm t bs =
  let ids = case t of
              SendTo ids -> ids
              SendAll -> getRemoteNodeIds bsm
  in traverse_ (\i -> putIntoOutbox bsm i bs) ids

checkAllSent :: BsmMulti -> STM Bool
checkAllSent bsm =
  let f (_,ob,_) = emptyOutbox ob
  in and <$> traverse f (Map.elems $ bsmTargets bsm)

putIntoOutbox :: BsmMulti -> NodeId -> ByteString -> STM ()
putIntoOutbox sock n bs = case Map.lookup n (bsmTargets sock) of
  Just (_,q,_) -> writeBsmOutbox q bs
  Nothing -> error $ "Called putIntoOutbox for unknown: " ++ show n

initBsmMulti :: Debugger -> NodeId -> Map NodeId MyAddr -> IO (BsmMulti)
initBsmMulti d self ns = do
  let mkTarget (n,addr) = do
        status <- newTVarIO False
        ob <- newBsmOutbox
        return (n,(addr,ob,status))
  inbox <- newBsmInbox
  targets <- Map.fromList
    <$> mapM mkTarget (Map.toList (Map.delete self ns))
  return $ BsmMulti
    { bsmDbg = d
    , bsmInbox = inbox
    , bsmTargets = targets
    , bsmSelf = self
    }

openBsmServer :: Debugger -> NodeId -> MyAddr -> BsmMulti -> IO ()
openBsmServer d selfNodeId selfAddr bsm = do
  forkIO $ runServer d selfAddr (bsmInbox bsm) (Map.map (\(_,ob,status) -> (ob,status)) (bsmTargets bsm))
  return ()

-- | First retry delay for connection, in microseconds
initialDelay :: Int
initialDelay = 1000 -- 1 ms

openBsmClients :: Debugger -> NodeId -> BsmMulti -> IO ()
openBsmClients d selfNode bsm = do
  let f targetNode (addr,ob,status) =
        if selfNode > targetNode
        then do
          debug d $ "Running client for " ++ show targetNode
          forkIO (runClient initialDelay d selfNode targetNode addr (bsmInbox bsm) ob status)
          return ()
        else return ()
  Map.traverseWithKey f (bsmTargets bsm)
  return ()

runBsmMulti :: Debugger -> NodeId -> Map NodeId MyAddr -> IO (BsmMulti)
runBsmMulti d self addrs = do
  let selfAddr = case Map.lookup self addrs of
        Just addr -> addr
        Nothing -> error "No addr for self"
  bsm <- initBsmMulti d self addrs
  openBsmServer d self selfAddr bsm
  openBsmClients d self bsm
  return bsm

testNetwork :: Map NodeId MyAddr
testNetwork = Map.fromList
  [(NodeId 0,MyAddr "127.0.0.1" "8050")
  ,(NodeId 1,MyAddr "127.0.0.1" "8051")
  ,(NodeId 2,MyAddr "127.0.0.1" "8052")
  ,(NodeId 3,MyAddr "127.0.0.1" "8053")
  ]

testBsmMulti :: Word32 -> [Word32] -> IO ()
testBsmMulti selfN otherN = do
  dbg <- runQD
  testBsmMulti' dbg selfN otherN

testBsmMulti' :: Debugger -> Word32 -> [Word32] -> IO ()
testBsmMulti' dbg selfN otherN = do
  let self = NodeId selfN
      others = map NodeId otherN
  let waitForReady bsm = atomically $ do
        status <- allReady bsm
        if status
          then return ()
          else retry
  let recvLoop bsm = do
        (sender,msg) <- atomically $ getFromInbox bsm
        debug dbg $ "From " ++ show sender ++ ": " ++ Store.decodeEx msg
        recvLoop bsm
  let sendLoop bsm n = do
        traverse_
          (\other -> atomically $ putIntoOutbox bsm other (Store.encode $ "Hello #" ++ show n))
          others
        debug dbg $ "Sent messages to " ++ show others
        threadDelay 1000000
        sendLoop bsm (n + 1)
  bsm <- runBsmMulti dbg self testNetwork
  debug dbg $ "Waiting for all nodes to be ready..."
  waitForReady bsm
  forkIO $ sendLoop bsm 1
  recvLoop bsm

test4 :: IO ()
test4 = do
  d <- runQD
  forkIO $ testBsmMulti' d 0 [1,2,3]
  forkIO $ testBsmMulti' d 1 [0,2,3]
  forkIO $ testBsmMulti' d 2 [0,1,3]
  testBsmMulti' d 3 [0,1,2]
