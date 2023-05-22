# pigeon


## IDがずれて取得失敗しているときのメモ
```sql
INSERT INTO item_detail_tbl(id,world,datetime,item_name,cost,unit_cost,count,update_time)
VALUES(13339607,'NULL',NOW(),'NULL',1,1,1,NOW()); --- 1マイナスのIDでダミー登録
COMMIT;
--- crawl
DELETE FROM item_detal_tbl WHERE id=13339607;
COMMIT;
```