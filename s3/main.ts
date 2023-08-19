import { GetObjectCommand, S3Client } from "@aws-sdk/client-s3"
import { createWriteStream } from "fs"
import * as dotenv from 'dotenv'
dotenv.config()

const downloadChunkSize = 1024 * 1024 * 20 // 20MB

main()

// S3のデータを20MBずつ分割しながらダウンロードし、ファイルも分割する
async function main() {
  const bucket = process.env.BUCKET_NAME
  const key = process.env.OBJECT_KEY
  const tmpFilePathName = './tmp/tmp_0.mp3'

  if (!bucket || !key) throw new Error('bucket or object key not set.')

  // 書き込むファイル名を指定
  const writeStream = createWriteStream(tmpFilePathName)

  // S3からoneMB分のデータを取得
  const { Body, ContentRange } = await getObjectRange({
    bucket,
    key,
    start: 0,
    end: downloadChunkSize,
  })

  // ファイル書き込み
  writeStream.write(await Body?.transformToByteArray())
  writeStream.close()

  // 取得したデータ量と全体のデータ量を取得
  // while文の条件式を統一するために、rangeAndLengthにまとめる
  let restRangeAndLength = getRangeAndLength(ContentRange)
  console.log(`download ${0} - ${downloadChunkSize} bytes (total: ${restRangeAndLength.length}).`)

  let index = 1
  while (!checkIsDownloadCompleted(restRangeAndLength)) {
    const writeStream = createWriteStream(`./tmp/tmp_${index}.mp3`)
    index++

    const currentStartBytePoint = restRangeAndLength.end + 1
    const currentEndBytePoint = restRangeAndLength.end + downloadChunkSize

    const { Body, ContentRange } = await getObjectRange({
      bucket,
      key,
      start: currentStartBytePoint,
      end: currentEndBytePoint
    })

    // データ作成
    writeStream.write(await Body?.transformToByteArray())
    writeStream.close()

    // S3のレスポンスから残りのデータ量を計算
    restRangeAndLength = getRangeAndLength(ContentRange)
    console.log(`download ${currentStartBytePoint}-${currentEndBytePoint} bytes (total: ${restRangeAndLength.length}).`)
  }
}

// ダウンロードが完了したかどうかの確認
function checkIsDownloadCompleted({ end, length }: { end: number; length: number }) {
  // contentRangeは次のような形式: start-end/length
  // ex. "bytes 0-1048575/1048576"
  // endはlengthより1小さい値になる
  return end === length - 1
}

// S3のレスポンスから取得したデータの幅や全体のデータ量を取得する
function getRangeAndLength(contentRange: any) {
  // contentRange: "bytes 0-1048575/1048576"
  const [range, length] = contentRange.split("/")
  const [start, end] = range.split("-")

  return {
    start: parseInt(start),
    end: parseInt(end),
    length: parseInt(length)
  }
}

// データを分割しながら、S3からデータを取得する
function getObjectRange({ bucket, key, start, end }: { bucket: string; key: string; start: number; end: number }) {
  const accessKeyId = process.env.AWS_ACCESS_KEY_ID
  const secretAccessKey = process.env.AWS_SECRET_ACCESS_KEY

  if (!accessKeyId || !secretAccessKey) throw new Error('aws keys not found.')

  const s3Client = new S3Client({
    credentials: { accessKeyId, secretAccessKey },
    region: "ap-northeast-1",
  })

  const command = new GetObjectCommand({
    Bucket: bucket,
    Key: key,
    Range: `bytes=${start}-${end}`
  })

  return s3Client.send(command)
}
