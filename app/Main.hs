module Main where

import Config
import State

import qualified Network.Ccm as Ccm
import Network.Ccm.Lens

import Control.Concurrent (forkIO, threadDelay)
import Control.Concurrent.STM
import Control.Monad.State
import Data.Map (Map)
import qualified Data.Map as Map
import Data.Set (Set)
import qualified Data.Set as Set
import qualified Data.Store as Store
import System.Environment (getArgs)
import Data.Time.Clock
import Data.Time.Format

main :: IO ()
main = do
  confFile <- getArgs >>= \as -> case as of
    [confFile] -> return confFile
    [] -> error "Called without config file argument"
    _ -> error "Called with more than one argument"
  conf <- inputConfig confFile
  putStrLn $ "Running " ++ show (conf ^. cNodeId)
  (errors,td) <- runExT nodeScript mkNoDebugDbg conf
  putStrLn $ "Causal deferrals: " ++ show errors
  putStrLn $ "Finished in " ++ showResultSeconds td

showResultSeconds :: NominalDiffTime -> String
showResultSeconds = formatTime defaultTimeLocale "%3Ess"

nodeScript :: ExT IO (Int, NominalDiffTime)
nodeScript = do
  sto <- use $ stConf . cExpr . cSetupTimeout
  result <- lift . atomicallyCcmTimedMicros (fmap (* 1000) sto) $ do
    status <- allReadyCcm
    stmCcm $ check status
  case result of
    Nothing -> error "Timeout during setup"
    Just () -> do
      t0 <- liftIO $ getCurrentTime
      self <- lift getSelf
      others <- lift getOthers
      nodeLoop
      -- Wait until all sent messages are actually transmitted.
      lift . atomicallyCcm $ awaitAllSent
      t1 <- liftIO $ getCurrentTime
      let td = diffUTCTime t1 t0
      errors <- use stSeenErrors
      return (errors,td)

checkAllDone :: (Monad m) => ExT m Bool
checkAllDone = do
  self <- lift getSelf
  others <- Set.toList <$> lift getOthers
  ocs <- use stReceived
  total <- use $ stConf . cExpr . cMsgCount
  next <- use $ stNextSend
  let
    f o = case Map.lookup o ocs of
      Just n -> n >= total - 1
      Nothing -> total == 0
    allSent = next >= total
    allReceived = and (map f others)
  return $ allSent && allReceived

nodeLoop :: ExT IO ()
nodeLoop = checkAllDone >>= \r -> if r
  then do
    return ()
  else do
    rto <- use $ stConf . cExpr . cRecvTimeout
    next <- use stNextSend
    limit <- use $ stConf . cExpr . cMsgCount

    result <- lift . atomicallyCcmTimedMicros (fmap (* 1000) rto) $
      orElseCcm nodeRecv (nodeSend next limit)

    case result of
      Nothing -> do
        rs <- use stReceived
        error $ "Timeout during experiment, " ++ show rs
      Just (Sent _) -> do
        stNextSend += 1
      Just (Received msgs) -> do
        accMsgs msgs
      Just (ReceivedError e) -> do
        stSeenErrors += 1
        self <- lift getSelf
        liftIO . putStrLn $ showCausalError' self e

    nodeLoop

accMsgs :: (Monad m) => [ExMsg] -> ExT m ()
accMsgs [] = return ()
accMsgs ((sender,n):ms) = do
  stReceived . at sender %= \v -> case v of
    Just n' | n' > n -> Just n'
    _ -> Just n
  accMsgs ms

data ActionResult
  = Sent Int
  | Received [ExMsg]
  | ReceivedError CausalError

nodeRecv :: CcmT STM ActionResult
nodeRecv = do
  undefined
  -- result <- (fmap . map $ Store.decodeEx) <$> recvCcm
  -- case result of
  --   Right msgs ->
  --     return $ Received msgs
  --   Left e ->
  --     return $ ReceivedError e

nodeSend :: Int -> Int -> CcmT STM ActionResult
nodeSend next total | next >= total = stmCcm retry
nodeSend next _ = do
  self <- getSelf
  let msg = (self, next)
  Ccm.blockSend (Store.encode msg)
  return $ Sent next
