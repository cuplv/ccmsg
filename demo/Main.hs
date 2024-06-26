{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}

import qualified Network.Framed as Framed

import Control.Concurrent (threadDelay)
import Control.Monad.Except
import qualified Network.Simple.TCP as TCP
import System.Environment (getArgs)

hello = mconcat $ replicate 5500 "Hello world!"

goodbye = "Goodbye world!"

main :: IO ()
main = getArgs >>= \case
  ["send"] -> sender
  ["recv"] -> recver

sender :: IO ()
sender = TCP.connect "127.0.0.1" "7720" $ \(sock,_) -> do
  let
    loop = do
      Framed.send sock hello
      liftIO $ putStrLn "Sent the message."
      liftIO $ threadDelay 200000
      loop
  result <- runExceptT loop
  print (result :: Either Framed.Exception ())

recver = TCP.serve "127.0.0.1" "7720" $ \(sock,_) -> do
  let
    loop = do
      Framed.recv sock >>= \case
        msg | msg == hello -> do
          liftIO $ putStrLn "Got the message."
          loop
        _ -> do
          liftIO $ putStrLn "Got some other message."
          loop
  result <- runExceptT loop
  print (result :: Either Framed.Exception ())
