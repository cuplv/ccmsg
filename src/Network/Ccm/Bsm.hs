module Network.Ccm.Bsm
  ( MyAddr (..)
  , SendTarget (..)
  , Bsm
  , runBsm
  , isEmptyInbox
  , readInbox
  , tryReadInbox
  , sendBsm
  , shutdownBsm
  , getSelfId
  , getPeerIds
  , getNotReady
  , allReady
  ) where

import Network.Ccm.Bsm.Internal
import Network.Ccm.Bsm.TCP
import Network.Ccm.Lens
import Network.Ccm.Types

import Control.Concurrent (forkIO)
import Data.Foldable (traverse_)
import Data.Map (Map)
import qualified Data.Map as Map
import Data.Set (Set)
import qualified Data.Set as Set

getSelfId :: Bsm -> NodeId
getSelfId = view bsmSelf

openBsmServer :: MyAddr -> Bsm -> IO ()
openBsmServer selfAddr bsm = do
  forkIO $ runServer selfAddr bsm
  return ()

-- | First retry delay for connection, in microseconds
initialDelay :: Int
initialDelay = 1000 -- 1 ms

openBsmClients :: Bsm -> Map NodeId MyAddr -> IO ()
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

runBsm :: Debugger -> NodeId -> Map NodeId MyAddr -> IO (Bsm)
runBsm d self addrs = do
  let selfAddr = case Map.lookup self addrs of
        Just addr -> addr
        Nothing -> error "No addr for self"
  bsm <- initBsm d self (Map.keysSet addrs)
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
