import Redis from "ioredis"
import * as R from "ramda"

export default (host = "localhost", port = 6379) => {
  const redis = new Redis(port, host)
  const STANDARD_STATUSES = ["complete", "trigger"]
  const CONSUMERGROUP = "consumers"
  const CONSUMERNAME = "consumer"
  const STREAMARGS = () => Array(STREAMS.length).fill(">")

  const addStreams = (type, atom) =>
    STANDARD_STATUSES.map((s) => `${type}:${atom}:${s}`)

  const createGroup = (stream) =>
    redis.xgroup("CREATE", stream, CONSUMERGROUP, 0, "MKSTREAM")

  const getGroups = async (stream) => {
    try {
      const groupInfo = await redis.xinfo("GROUPS", stream)
      const hasGroup = groupInfo.some((g) => g[1] === CONSUMERGROUP)
      return hasGroup ? true : createGroup(stream)
    } catch (e) {
      return createGroup(stream)
    }
  }

  const processStreamKey =
    (fn) =>
    ([key, data]) => {
      const [streamId, ...fields] = data
      const kv = R.fromPairs(
        fields[0].reduce((result, _, index, array) => {
          if (index % 2 === 0) {
            result.push(array.slice(index, index + 2))
          }
          return result
        }, [])
      )
      console.log("data", key, streamId, kv)
      fn(key, streamId, kv)
      return redis.xack(key, CONSUMERGROUP, streamId)
    }
  return {
    addStreamData(type, atom, status, data) {
      redis.xadd(
        `${type}:${atom}:${status}`,
        "*",
        ...Object.entries(data).flat()
      )
    },
    client: redis,
    addStreams,
    addNewStream(type, name) {
      return Promise.all(addStreams(type, name).map(getGroups))
    },
    addListener(type, name, status) {
      STREAMS = [...STREAMS, `${type}:${name}:${status}`]
    },
    async listenForMessagesfn(fn) {
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
