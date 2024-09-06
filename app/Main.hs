module Main where

import Config
import State

import Control.Monad.DebugLog
import qualified Network.Ccm as Ccm
import qualified Network.Ccm.Extra as Extra
-- import Network.Ccm.State (ccmStats,totalOutOfOrder)
import Network.Ccm.Lens

import Control.Concurrent (forkIO, threadDelay)
import Control.Concurrent.STM
import Control.Monad (filterM,when)
import Control.Monad.State
import Data.ByteString (ByteString)
import Data.Either (fromRight)
import Data.Map (Map)
import qualified Data.Map as Map
import Data.Set (Set)
import qualified Data.Set as Set
import Data.Sequence (Seq)
import qualified Data.Sequence as Seq
import qualified Data.Store as Store
import System.Environment (getArgs)
import System.Random
import Data.Time.Clock
import Data.Time.Format

main :: IO ()
main = do
  confFile <- getArgs >>= \as -> case as of
    [confFile] -> return confFile
    [] -> error "Called without config file argument"
    _ -> error "Called with more than one argument"
  conf <- inputConfig confFile
  let ds = (fromRight (error "parse fail") . parseDebugSelector $ "*") :: Selector

  flip runLogStdoutC (Set.singleton ds) $ do
    dlog ["trace"] $ "Running " ++ show (conf ^. cNodeId)
    (td) <- runExM nodeScript conf
    -- putStrLn $ "Causal deferrals: " ++ show errors
    liftIO.putStrLn $ "Finished in " ++ showResultSeconds td

showResultSeconds :: NominalDiffTime -> String
showResultSeconds = formatTime defaultTimeLocale "%3Ess"

atomicallyTimed :: (MonadIO m) => Int -> STM a -> m (Maybe a)
atomicallyTimed us m = do
  v <- liftIO $ newEmptyTMVarIO
  tid <- liftIO $ forkIO $ do
    threadDelay us
    atomically $ putTMVar v ()
  liftIO . atomically $
    (Just <$> m)
    `orElse`
    (const Nothing <$> takeTMVar v)

{- | Repeat the monadic action until it returns a 'Just' value. -}
untilJust :: (Monad m) => m (Maybe a) -> m a
untilJust m = do
  result <- m
  case result of
    Just a -> return a
    Nothing -> untilJust m

{- | Repeatedly perform the given 'STM' action, feeding the result into
   the given function, until the given microseconds elapse.

   The timer can only preempt the 'STM' action, so if the body is
   long-running, the loop could end up running for significantly
   longer than the given time limit. -}
loopForMicros :: Int -> STM a -> (a -> ExM ()) -> ExM ()
loopForMicros us input body = do
  v <- liftIO newEmptyTMVarIO
  liftIO.forkIO $ do
    threadDelay us
    atomically $ putTMVar v ()

  untilJust $ do
    result <- liftIO.atomically $
      -- Timer gets priority
      (const Nothing <$> takeTMVar v)
      `orElse`
      (Just <$> input)
    case result of
      -- Timer has expired, end loop.
      Nothing -> return (Just ())
      -- Timer is still running, evaluate the body and continue loop.
      Just a -> body a >> return Nothing

nodeScript :: ExM (NominalDiffTime)
nodeScript = do
  sto <- use $ stConf . cExpr . cSetupTimeout
  testReady <- lift Extra.allPeersReady
  result <- case sto of
    Just ms -> atomicallyTimed (ms * 1000) (check =<< testReady)
    Nothing -> liftIO.atomically $ Just <$> (check =<< testReady)
  case result of
    Nothing -> error "Timeout during setup"
    Just () -> do
      t0 <- liftIO $ getCurrentTime
      self <- lift Ccm.getSelf
      dlog ["trace"] $ "Entering main loop"
      nodeLoop
      t1 <- liftIO $ getCurrentTime
      let td = diffUTCTime t1 t0
      -- Exchange for 1s more to make sure everyone finishes.
      testReady <- lift $ Ccm.readyForExchange
      loopForMicros 1000000 testReady $ \_ -> do
        lift Ccm.exchange
        return ()

      return (td)

checkAllDone :: ExM Bool
checkAllDone = do
  self <- lift Ccm.getSelf
  peers <- Set.toList <$> lift Ccm.getPeers
  ocs <- use stReceived
  dlog ["trace"] $ "Checking done: " ++ show ocs
  total <- use $ stConf . cExpr . cMsgCount
  next <- use $ stNextSend
  let
    f o = case Map.lookup o ocs of
      Just n -> n >= total - 1
      Nothing -> total == 0
    allReceived = and (map f peers)
  if not allReceived
    then dlog ["trace"] $ "Not done."
    else dlog ["trace"] $ "Done."
  return $ allReceived

nodeLoop :: ExM ()
nodeLoop = untilJust $ do
  self <- lift Ccm.getSelf
  total <- use $ stConf.cExpr.cMsgCount
  next <- use stNextSend

  -- Publish if we have not hit limit
  when (next < total) $ do
    lift $ Ccm.publish (Store.encode next)
    stNextSend += 1

  -- Exchange for 10ms
  testReady <- lift $ Ccm.readyForExchange
  loopForMicros 10000 testReady $ \_ -> do
    newPosts <- (lift Ccm.exchange) :: ExM (Seq (Ccm.NodeId, ByteString))
    -- Decode and record receipt of posts
    accPosts (newPosts & each . _2 %~ Store.decodeEx)

  continue <- not <$> checkAllDone
  if continue
    then return Nothing
    else return $ Just ()

accPosts :: Seq (Ccm.NodeId, ExMsg) -> ExM ()
accPosts Seq.Empty = return ()
accPosts ((creator,n) Seq.:<| ms) = do
  stReceived . at creator %= \v -> case v of
    Just n' | n' > n -> Just n'
    _ -> Just n
  accPosts ms
