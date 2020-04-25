# covid19_shizuoka-opendata_notifier

https://github.com/code-for-hamamatsu/covid19_shizuoka-opendata_csv2json
このAPIをポーリングして、データにアップデートがあれば以下を行うプロジェクトです。
* S3にオリジナルデータ(CSV)と取得データ(JSON)を保存する。
* DynamoDBの最新テーブル情報を更新する。
* DynamoDBの履歴テーブルへ挿入する。
* Slackへ通知する。
