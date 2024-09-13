\(nodeId : Natural) ->

{ nodeId
, debugLog = ["progress"]
, experiment =
  { msgCount = 100
  , recvTimeout = Some 5000
  , setupTimeout = Some 10000
  , sendChance = Some 0.7
  , missingLinks = False
  , network =
      [ { id = 0, host = "127.0.0.1", port = "8050" }
      , { id = 1, host = "127.0.0.1", port = "8051" }
      , { id = 2, host = "127.0.0.1", port = "8052" }
      , { id = 3, host = "127.0.0.1", port = "8053" }
      ]
  }
}
