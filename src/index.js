import Redis from "ioredis"
import * as R from "ramda"

export default (host = "localhost", port = 6379) => {
  const redis = new Redis(port, host)
  const unblockedRedis = new Redis(port, host)
  const CONSUMERGROUP = "consumers"
  const CONSUMERNAME = "consumer"
  let STREAMS = []
  const STREAMARGS = () => Array(STREAMS.length).fill(">")

  const createGroup = (stream) =>
    unblockedRedis.xgroup("CREATE", stream, CONSUMERGROUP, 0, "MKSTREAM")

  const createStreamGroup = async (stream) => {
    try {
      const groupInfo = await unblockedRedis.xinfo("GROUPS", stream)
      const hasGroup = groupInfo.some((g) => g[1] === CONSUMERGROUP)
      return hasGroup ? true : createGroup(stream)
    } catch (e) {
      return createGroup(stream)
    }
  }

  const processStreamKey =
    (fn) =>
    ([key, data]) => {
      const [streamId, ...fields] = data[0]
      const kv = R.fromPairs(
        fields[0].reduce((result, _, index, array) => {
          if (index % 2 === 0) {
            result.push(array.slice(index, index + 2))
          }
          return result
        }, [])
      )
      fn(key, streamId, kv)
      return unblockedRedis.xack(key, CONSUMERGROUP, streamId)
    }
  return {
    addStreamData(streamName, data) {
      return unblockedRedis.xadd(
        streamName,
        "*",
        ...(Array.isArray(data) ? data : Object.entries(data).flat())
      )
    },
    client: unblockedRedis,
    createStreamGroup,
    addListener(streamName) {
      STREAMS = [...STREAMS, streamName]
    },
    async listenForMessages(fn) {
      const messages = await redis.xreadgroup(
        "GROUP",
        CONSUMERGROUP,
        CONSUMERNAME,
        "BLOCK",
        0,
        "STREAMS",
        ...STREAMS,
        ...STREAMARGS()
      )
      return Promise.all(messages.flatMap(processStreamKey(fn)))
    },
  }
}
