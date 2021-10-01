import Redis from "ioredis"
import * as R from "ramda"
import { map, tryCatch, pipe, reduce, curry, filter, flatMap } from "rubico"

export default (host = "localhost", port = 6379) => {
  const redis = new Redis(port, host)
  const STANDARD_STATUSES = ["complete", "trigger"]
  const CONSUMERGROUP = "consumers"
  const CONSUMERNAME = "consumer"
  const ATOMS = ["http", "db", "data"]
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
    (stream) => createGroup(stream)
  )

  const arrayToPairs = reduce((result, _, index, array) => {
    if (index % 2 === 0) {
      result.push(array.slice(index, index + 2))
    }
    return result
  }, [])

  try {
    const streams = await redis.scan("0", "type", "STREAM")
    STREAMS = [
      ...new Set(
        pipe(
          flatMap((a) => addStreams("atom", a)),
          (result) => [...result, ...STANDARD_STREAMS, ...streams[1]]
        )(ATOMS)
      ),
    ]
    await Promise.all(map(getGroups)(STREAMS))
  } catch (e) {
    console.error("Stream error", e)
    process.exit(1)
  }
  const processStreamKey =
    (fn) =>
    ([key, data]) => {
      const [streamId, ...fields] = data
      const kv = R.fromPairs(arrayToPairs(fields[0]))
      console.log("data", key, streamId, kv)
      return fn(key, streamId, kv)
    }
  return {
    addStreamData(type, atom, status, data) {
      redis.xadd(
        `${type}:${atom}:${status}`,
        "*",
        ...Object.entries(data).flat()
      )
    },
    addNewStream(type, atom) {
      const newStreams = addStreams(type, atom)
      STREAMS = [...newStreams, ...STREAMS]
      return Promise.all(map(getGroups)(newStreams))
    },
    listenForMessages(fn) {
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
