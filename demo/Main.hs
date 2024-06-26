{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}

import qualified Network.Framed as Framed

import Control.Concurrent (threadDelay)
import Control.Monad.Except
-- import Control.Monad.IO.Class
import qualified Network.Simple.TCP as TCP
import System.Environment (getArgs)

hello = "Hello world!"

goodbye = "Goodbye world!"

main :: IO ()
main = getArgs >>= \case
  ["send"] -> sender
  ["recv"] -> recver

sender :: IO ()
sender = TCP.connect "127.0.0.1" "7720" $ \(sock,_) -> do
  let
    loop = do
      Framed.sendStrict sock hello
      liftIO $ putStrLn "Sent the message."
      liftIO $ threadDelay 200000
      loop
  result <- runExceptT loop
  print (result :: Either Framed.NetworkException ())

recver = TCP.serve "127.0.0.1" "7720" $ \(sock,_) -> do
  let
    loop = do
      runExceptT (Framed.recv sock) >>= \case
        Right msg | msg == hello -> liftIO $ putStrLn "Got the message."
        Right msg -> liftIO $ putStrLn "Got some other message."
        Left e -> liftIO $ print e
      liftIO $ threadDelay 1000000
      loop
  result <- runExceptT loop
  print (result :: Either Framed.NetworkException ())
