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
main = getArgs >>= \case
  ["send"] -> sender
  ["recv"] -> recver
  ["both"] -> both

sender :: IO ()
sender = TCP.connect "127.0.0.1" "7720" $ \(sock,_) -> do
  let
    loop = do
      Framed.send sock hello
      dlog 1 "Sent the message."
      liftIO $ threadDelay 200000
      loop
  result <- runExceptT (runLogStdout (loop) 1 "sender")
  print (result :: Either Framed.Exception ())

recver = TCP.serve "127.0.0.1" "7720" $ \(sock,_) -> do
  let
    recvLoop = do
      Framed.recv sock >>= \case
        msg | msg == hello -> do
          dlog 1 "Got the message."
          recvLoop
        _ -> do
          dlog 1 "Got some other message."
          recvLoop
  result <- runExceptT (runLogStdout recvLoop 1 "recver")
  print (result :: Either Framed.Exception ())

both :: IO ()
both = do
  q <- newTQueueIO

  forkIO $ TCP.serve "127.0.0.1" "7720" $ \(sock,_) -> do
    let
      recvLoop = do
        Framed.recv sock >>= \case
          msg | msg == hello -> do
            dlog 1 "Got the message."
            recvLoop
          _ -> do
            dlog 1 "Got some other message."
            recvLoop
    runExceptT (runLogTQueue recvLoop 1 q)
    return ()

  liftIO $ threadDelay 200000

  forkIO $ TCP.connect "127.0.0.1" "7720" $ \(sock,_) -> do
    let
      sendLoop = do
        Framed.send sock hello
        dlog 1 "Sent the message."
        liftIO $ threadDelay 200000
        sendLoop
    runExceptT (runLogTQueue sendLoop 1 q)
    return ()

  let
    logLoop = do
      s <- liftIO . atomically $ readTQueue q
      liftIO $ putStrLn s
      logLoop

  logLoop
