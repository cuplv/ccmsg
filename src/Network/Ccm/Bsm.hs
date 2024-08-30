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

import Control.Monad.DebugLog
import Network.Ccm.Bsm.Internal
import Network.Ccm.Bsm.TCP
import Network.Ccm.Lens
import Network.Ccm.Types

import Control.Monad.IO.Class (liftIO)
import Control.Concurrent (forkIO)
import Data.Foldable (traverse_)
import Data.Map (Map)
import qualified Data.Map as Map
import Data.Set (Set)
import qualified Data.Set as Set

getSelfId :: Bsm -> NodeId
getSelfId = view bsmSelf

openBsmServer :: MyAddr -> Bsm -> LogIO IO ()
openBsmServer selfAddr bsm = do
  forkLogIO $ runServer selfAddr bsm
  return ()

-- | First retry delay for connection, in microseconds
initialDelay :: Int
initialDelay = 1000 -- 1 ms

openBsmClients :: Bsm -> Map NodeId MyAddr -> LogIO IO ()
openBsmClients bsm addrs = do
  let
    f :: NodeId -> LogIO IO ()
    f targetNode =
      if (bsm^.bsmSelf) > targetNode
      then do
        let addr = case Map.lookup targetNode addrs of
              Just a -> a
              Nothing -> error $ "Cannot open client, no addr for: " ++ show targetNode
        dlog ["trace"] $ "Running client for " ++ show targetNode
        forkLogIO $ runClient initialDelay bsm targetNode addr
        return ()
      else return ()
  traverse_ f (Set.toList (getPeerIds bsm))
  return ()

runBsm :: Debugger -> NodeId -> Map NodeId MyAddr -> LogIO IO Bsm
runBsm d self addrs = do
  let selfAddr = case Map.lookup self addrs of
        Just addr -> addr
        Nothing -> error "No addr for self"
  bsm <- liftIO $ initBsm d self (Map.keysSet addrs)
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
