{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}

import qualified Network.Framed as Framed
import Control.Monad.DebugLog

import Control.Concurrent (threadDelay, forkIO)
import Control.Concurrent.STM
import Control.Monad.Except
import qualified Network.Simple.TCP as TCP
import System.Environment (getArgs)

hello = mconcat $ replicate 4500 "Hello world!"

goodbye = "Goodbye world!"

main :: IO ()
main = do
  [lvl,command] <- getArgs
  flip runLogStdoutC (read lvl) $ do
    case command of
      "send" -> sender
      "recv" -> recver
      "both" -> do
        liftIO . forkIO =<< passLogIO sender
        recver
      _ -> dlog 0 "Unrecognized command"

sender :: LogIO Int IO ()
sender = do
  f <- passLogIOF $ \(sock,_) -> do
    let
      step = do
        Framed.send sock hello
        dlog 1 "Sent the message."
        liftIO $ threadDelay 200000
    result <- runExceptT $ forever step
    dlog 0 $
      "Sender ended with "
      ++ show (result :: Either Framed.Exception ())
  liftIO . TCP.connect "127.0.0.1" "7720" $ f

recver :: LogIO Int IO ()
recver = do
  f <- passLogIOF $ \(sock,_) -> do
    let
      step = do
        Framed.recv sock >>= \case
          msg | msg == hello -> do
            dlog 1 "Got the message."
          _ -> do
            dlog 1 "Got some other message."
    result <- runExceptT $ forever step
    dlog (0 :: Int) $
      "Receiver ended with "
      ++ show (result :: Either Framed.Exception ())
  liftIO . TCP.serve "127.0.0.1" "7720" $ f
