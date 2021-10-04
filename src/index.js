import Redis from "ioredis"
import * as R from "ramda"
import rubico from "rubico"

const { map, tryCatch, pipe, reduce, curry, filter } = rubico

export default (host = "localhost", port = 6379) => {
  const redis = new Redis(port, host)
  const STANDARD_STATUSES = ["complete", "trigger"]
  const CONSUMERGROUP = "consumers"
  const CONSUMERNAME = "consumer"
  const STREAMARGS = () => map((_) => ">")(Array(STREAMS.length))

  const addStreams = (type, atom) =>
    map((s) => `${type}:${atom}:${s}`)(STANDARD_STATUSES)

  const createGroup = (stream) =>
    redis.xgroup("CREATE", stream, CONSUMERGROUP, 0, "MKSTREAM")

  const getGroups = tryCatch(
    (stream) =>
      pipe(
        curry(redis.xinfo("GROUPS")),
        filter((g) => g[1] === CONSUMERGROUP),
        switchCase(
          (groups) => groups.length > 1,
          (groups) => groups,
          (_) => createGroup(stream)
        )
      )(stream),
    createGroup
  )

  const arrayToPairs = reduce((result, _, index, array) => {
    if (index % 2 === 0) {
      result.push(array.slice(index, index + 2))
    }
    return result
  }, [])

  const processStreamKey =
    (fn) =>
    ([key, data]) => {
      const [streamId, ...fields] = data
      const kv = R.fromPairs(arrayToPairs(fields[0]))
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
      const newStreams = addStreams(type, name)
      return Promise.all(map(getGroups)(newStreams))
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
