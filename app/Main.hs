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
import Control.Monad (filterM,when,forever)
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
import System.Exit (exitFailure)
import System.IO (stderr,hPutStrLn)
import System.Random
import Data.Time.Clock
import Data.Time.Format

-- Microseconds between progress logs
progressLogInterval :: Int
progressLogInterval = 3000000

pds :: String -> IO Selector
pds s = case parseDebugSelector s of
  Right ds -> return ds
  Left e -> do
    hPutStrLn stderr $
      "Debug selector \""
      ++ s
      ++ "\" could not be parsed: "
      ++ show e
    exitFailure

main :: IO ()
main = do
  confFile <- getArgs >>= \as -> case as of
    [confFile] -> return confFile
    [] -> error "Called without config file argument"
    _ -> error "Called with more than one argument"
  conf <- inputConfig confFile
  -- let ds = (fromRight (error "debug selector parse fail") . parseDebugSelector $ "trace") :: Selector
  dss <- Set.fromList <$> mapM pds (conf^.cDebugLog)
    -- dss = case parseDebugSelector (conf^.cDebugLog) of
    --   Right ds -> ds
    --   Left e -> error $ "Debug selector could not be parsed: " ++ show e
  td <- flip runLogStdoutC dss $ do
    dlog ["trace"] $ "Running " ++ show (conf ^. cNodeId)
    runExM nodeScript conf

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

{- | Repeatedly perform the given 'ExM (STM a)' action, feeding the
   result into the given function, until the given microseconds
   elapse.

   The timer can only preempt the 'STM' action, so if the body is
   long-running, the loop could end up running for significantly
   longer than the given time limit. -}
loopForMicros :: Int -> ExM (STM a) -> (a -> ExM ()) -> ExM ()
loopForMicros us input body = do
  v <- liftIO newEmptyTMVarIO
  liftIO.forkIO $ do
    threadDelay us
    atomically $ putTMVar v ()

  untilJust $ do
    inputTest <- input
    result <- liftIO.atomically $
      -- Timer gets priority
      (const Nothing <$> takeTMVar v)
      `orElse`
      (Just <$> inputTest)
    case result of
      -- Timer has expired, end loop.
      Nothing -> return (Just ())
      -- Timer is still running, evaluate the body and continue loop.
      Just a -> body a >> return Nothing

nodeScript :: ExM (NominalDiffTime)
nodeScript = do
  sto <- use $ stConf . cExpr . cSetupTimeout
  sendChance <- use $ stConf . cExpr . cSendChance
  case sendChance of
    Just d -> lift $ Extra.setTransmissionMode (Extra.TMLossy d)
    Nothing -> return ()
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

      statusTask <- liftIO newEmptyTMVarIO
      statusDone <- liftIO newEmptyTMVarIO
      liftIO.forkIO.forever $ do
        atomically $ putTMVar statusTask ()
        atomically $ takeTMVar statusDone
        threadDelay progressLogInterval -- 1ms

      nodeLoop statusTask statusDone
      t1 <- liftIO $ getCurrentTime
      let td = diffUTCTime t1 t0

      -- Keep exchanging until everyone has finished.
      untilJust $ do
        getExchange <- lift $ Ccm.awaitExchange
        e <- liftIO.atomically $ getExchange
        dlog ["exchange"] $ "Post-completion exchange: " ++ show e
        lift $ Ccm.exchange e
        done <- lift Ccm.allPeersUpToDate
        if done
          then return $ Just ()
          else return Nothing

      -- Exchange for 100ms more so that everyone knows that everyone
      -- is finished.
      loopForMicros 10000 (lift Ccm.awaitExchange) $ \e -> do
        lift $ Ccm.exchange e
        return ()

      return td

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
  return $ allReceived && next >= total

nodeLoop :: TMVar () -> TMVar () -> ExM ()
nodeLoop statusTask statusDone = untilJust $ do
  self <- lift Ccm.getSelf
  total <- use $ stConf.cExpr.cMsgCount
  next <- use stNextSend

  -- Publish if we have not hit limit
  when (next < total) $ do
    lift $ Ccm.publish (Store.encode next)
    oc <- lift Extra.getOutputPostClock
    dlog ["post"] $
      "Published post "
      ++ show next
      ++ ", clock is now "
      ++ show oc
    stNextSend += 1

  -- Exchange for 10ms
  loopForMicros 10000 (lift Ccm.awaitExchange) $ \e -> do
    newPosts <- lift $ Ccm.exchange e
    -- Decode and record receipt of posts
    accPosts (newPosts & each . _2 %~ Store.decodeEx)

  statusTime <- liftIO.atomically $ tryTakeTMVar statusTask
  case statusTime of
    Just () -> do
      -- print status, replace flag
      recvd <- use stReceived
      dlog ["progress"] $ show recvd
      liftIO.atomically $ putTMVar statusDone ()
    Nothing -> return ()

  continue <- not <$> checkAllDone
  if continue
    then return Nothing
    else return $ Just ()

accPosts :: Seq (Ccm.NodeId, ExMsg) -> ExM ()
accPosts Seq.Empty = return ()
accPosts ((creator,n) Seq.:<| ms) = do
  dlog ["post"] $
    "Got post "
    ++ show n
    ++ " from node "
    ++ show creator
  stReceived . at creator %= \v -> case v of
    Just n' | n' > n -> Just n'
    _ -> Just n
  accPosts ms
