# covid19_shizuoka-opendata_notifier

https://github.com/code-for-hamamatsu/covid19_shizuoka-opendata_csv2json
  
このAPIをコールして、データにアップデートがあれば以下を行うプロジェクトです。
* S3にオリジナルデータ(CSV)と取得データ(JSON)を保存する。
* DynamoDBの最新テーブル情報を更新する。(レコードがなければ挿入する)
* DynamoDBの履歴テーブルへ挿入する。
* Slackへ通知する。


## ライブラリのインストール
$ pip install -r requirements.txt -t source

## パッケージング&デプロイ コマンド
$ find . | grep -E "(__pycache__|\.pyc|\.pyo$)" | xargs rm -rf
$ cd source
$ zip -r ../lambda-package.zip *
$ aws lambda update-function-code --function-name {{your function name}} --zip-file fileb://../lambda-package.zip
