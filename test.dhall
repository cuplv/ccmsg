\(nodeId : Natural) ->

{ nodeId
, experiment =
  { msgCount = 10000
  , recvTimeout = Some 5000
  , setupTimeout = Some 5000
  , network =
      [ { id = 0, host = "127.0.0.1", port = "8050" }
      , { id = 1, host = "127.0.0.1", port = "8051" }
      , { id = 2, host = "127.0.0.1", port = "8052" }
      , { id = 3, host = "127.0.0.1", port = "8053" }
      ]
  }
}
