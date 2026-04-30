import csv
import pymongo
import pymongo.errors
from datetime import datetime, timedelta
import sys
import os
import traceback
import json

# 新增：用于处理 .xlsx 文件
try:
    import openpyxl
except ImportError:
    pass # 留给后续逻辑中优雅地报错

CONFIG_FILE = "config.json"
VERSION = "4.2.1-Performance-UID"

def safe_date_format(dt_obj):
    """安全地转换日期，如果为空则返回空字符串"""
    if not dt_obj or not isinstance(dt_obj, datetime):
        return ""
    try:
        return (dt_obj + timedelta(hours=8)).strftime('%Y-%m-%d %H:%M:%S')
    except:
        return ""

def load_or_ask_config():
    config = {
        "mongo_uri": "",
        "db_name": "",
        "app_id": ""
    }

    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                config.update(json.load(f))
            print("✅ 已加载本地配置")
        except:
            print("⚠️ 配置文件读取失败，将重新输入")

    print("\n--- 基础配置 ---")

    if not config["mongo_uri"]:
        while not config["mongo_uri"]:
            config["mongo_uri"] = input("请输入 MongoDB 连接地址: ").strip()
    else:
        mongo_in = input("MongoDB 地址 (回车保持现状): ").strip()
        if mongo_in: config["mongo_uri"] = mongo_in

    if not config["db_name"]:
        while not config["db_name"]:
            config["db_name"] = input("请输入数据库名称: ").strip()
    else:
        db_in = input(f"数据库名称 [{config['db_name']}] (回车保持现状): ").strip()
        if db_in: config["db_name"] = db_in

    if not config["app_id"]:
        while not config["app_id"]:
            app_in = input("请输入业务 AppID (必须为数字): ").strip()
            if app_in.isdigit():
                config["app_id"] = int(app_in)
            else:
                print("❌ AppID 必须是数字")
    else:
        app_in = input(f"业务 AppID [{config['app_id']}] (回车保持现状): ").strip()
        if app_in:
            if app_in.isdigit():
                config["app_id"] = int(app_in)
            else:
                print("❌ AppID 必须是数字，已保留原配置")

    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(config, f, indent=4, ensure_ascii=False)

    return config

def run_report_1_chongti(db, config, start_utc, end_utc, date_str):
    output_file = f"充提数据_{config['db_name']}_{date_str}.csv"
    print(f"\n[1/4] 正在拉取基础订单 ({date_str})...")
    
    pipeline = [
        {"$match": {"appID": config['app_id'], "updatedAt": {"$gte": start_utc, "$lt": end_utc}, 
                    "type": {"$in": ['pay', 'withdrawal']}, "status": {"$in": ['Completed', 'MockCompleted']}, 
                    "ignoreAnalysis": {"$ne": True}}},
        {"$group": {"_id": {"user": "$user", "channel": {"$ifNull": ["$channel", 0]}},
                    "firstStatus": {"$first": "$status"},
                    "存款次数": {"$sum": {"$cond": [{"$eq": ["$type", "pay"]}, 1, 0]}},
                    "存款金额": {"$sum": {"$cond": [{"$eq": ["$type", "pay"]}, {"$ifNull": ["$totalPrice", 0]}, 0]}},
                    "提款金额": {"$sum": {"$cond": [{"$eq": ["$type", "withdrawal"]}, {"$ifNull": ["$totalPrice", 0]}, 0]}}}}
    ]
    order_results = list(db["orders"].aggregate(pipeline, allowDiskUse=True))
    
    if not order_results:
        print("⚠️ 该时间段未找到数据。")
        return

    uids = [res['_id']['user'] for res in order_results]
    print(f"[2/4] 同步 {len(uids)} 个用户...")
    user_map = {u['_id']: u for u in db["users"].find({"_id": {"$in": uids}}, {"uid": 1, "meta.adChannel": 1, "createdAt": 1})}
    
    daily_pipeline = [
        {"$match": {"user": {"$in": uids}, "startAt": {"$gte": start_utc, "$lt": end_utc}}},
        {"$group": {"_id": "$user", "rewardCash": {"$sum": "$rewardCash"}, "betAmount": {"$sum": "$betAmount"}}}
    ]
    daily_map = {res['_id']: res for res in db["transactiondailies"].aggregate(daily_pipeline)}

    print(f"[3/4] 写入文件...")
    with open(output_file, 'w', newline='', encoding='utf-8-sig') as f:
        writer = csv.writer(f)
        writer.writerow(["uid", "用户渠道", "注册日期", "是否模拟回调", "区间内存款金额", "区间内提款金额", "区间内存款次数", "区间内获得真金奖励", "区间内投注金额"])
        
        rows_to_write = []
        for doc in order_results:
            u_id = doc['_id']['user']
            u = user_map.get(u_id, {})
            d = daily_map.get(u_id, {})
            rows_to_write.append([
                u.get('uid', ''), "广告" if u.get('meta', {}).get('adChannel') else "自然裂变",
                safe_date_format(u.get('createdAt')),
                "是" if doc.get('firstStatus') == 'MockCompleted' else "否",
                doc.get('存款金额', 0), doc.get('提款金额', 0), doc.get('存款次数', 0),
                d.get('rewardCash', 0), d.get('betAmount', 0)
            ])
        writer.writerows(rows_to_write)
        
    print(f"✅ 完成: {os.path.abspath(output_file)}")

def run_report_2_shoucun(db, config, start_utc, end_utc, date_str):
    output_file = f"首存订单_{config['db_name']}_{date_str}.csv"
    print(f"\n[1/4] 正在统计该时间段所有充值用户 ({date_str})...")
    
    pay_pipeline = [
        {"$match": {"appID": config['app_id'], "updatedAt": {"$gte": start_utc, "$lt": end_utc}, 
                    "type": "pay", "status": "Completed", "ignoreAnalysis": {"$ne": True}}},
        {"$sort": {"updatedAt": 1, "_id": 1}},
        {"$group": {"_id": "$user", "次数": {"$sum": 1}, "总额": {"$sum": "$totalPrice"}, "第一笔": {"$first": "$totalPrice"}}}
    ]
    pay_results = list(db["orders"].aggregate(pay_pipeline, allowDiskUse=True))
    uids = [res['_id'] for res in pay_results]
    
    print(f"[2/4] 正在筛选在这几天内产生首次充值的用户...")
    shoucun_users = {u['_id']: u for u in db["users"].find({
        "_id": {"$in": uids},
        "meta.firstRechargeAt": {"$gte": start_utc, "$lt": end_utc}
    }, {"uid": 1, "meta.adChannel": 1, "meta.firstRechargeAt": 1, "createdAt": 1})}
    
    sc_uids = list(shoucun_users.keys())
    if not sc_uids:
        print("⚠️ 该时间段内无首存用户。")
        return

    print(f"[3/4] 抓取提款与日报数据 (共 {len(sc_uids)} 人)...")
    wd_pipeline = [{"$match": {"user": {"$in": sc_uids}, "type": "withdrawal", "status": "Completed", 
                               "updatedAt": {"$gte": start_utc, "$lt": end_utc}}},
                   {"$group": {"_id": "$user", "total": {"$sum": "$totalPrice"}}}]
    wd_map = {res['_id']: res['total'] for res in db["orders"].aggregate(wd_pipeline)}
    
    daily_pipeline = [
        {"$match": {"user": {"$in": sc_uids}, "startAt": {"$gte": start_utc, "$lt": end_utc}}},
        {"$group": {"_id": "$user", "rewardCash": {"$sum": "$rewardCash"}, "betAmount": {"$sum": "$betAmount"}}}
    ]
    daily_map = {res['_id']: res for res in db["transactiondailies"].aggregate(daily_pipeline)}

    print(f"[4/4] 导出报表...")
    headers = ["用户渠道", "uid", "注册日期", "首次充值日期", "区间内存款次数", "第一笔充值金额", "区间内总充值金额", "区间内提款金额", "区间内获得真金奖励", "区间内投注金额"]
    
    with open(output_file, 'w', newline='', encoding='utf-8-sig') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        
        rows_to_write = []
        for res in pay_results:
            uid = res['_id']
            if uid not in shoucun_users: continue
            u = shoucun_users[uid]
            d = daily_map.get(uid, {})
            rows_to_write.append([
                "广告" if u.get('meta', {}).get('adChannel') else "自然裂变",
                u.get('uid', ''),
                safe_date_format(u.get('createdAt')),
                safe_date_format(u.get('meta', {}).get('firstRechargeAt')),
                res.get('次数', 0), res.get('第一笔', 0), res.get('总额', 0),
                wd_map.get(uid, 0), d.get('rewardCash', 0), d.get('betAmount', 0)
            ])
        writer.writerows(rows_to_write)
        
    print(f"✅ 完成: {os.path.abspath(output_file)}")

def run_report_3_sms_recall(db, config, start_utc, end_utc, date_str):
    print(f"\n--- [书生计算 SMS 召回情况 ({date_str})] ---")
    file_name = input("请输入文件名 (支持 .csv 或 .xlsx，例如 target_users.xlsx): ").strip()
    
    if not os.path.exists(file_name):
        print(f"❌ 找不到文件: {file_name}，请确保它和本程序在同一个文件夹！")
        return

    target_uids = []
    ext = os.path.splitext(file_name)[1].lower()

    try:
        if ext == '.csv':
            with open(file_name, 'r', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f)
                uid_key = next((k for k in reader.fieldnames if str(k).strip().lower() == 'uid'), None)
                if not uid_key:
                    print("❌ CSV 文件的表头里没有找到 'uid' 或 'UID'！请检查。")
                    return
                for row in reader:
                    val = str(row[uid_key]).strip()
                    if val.isdigit():
                        target_uids.append(int(val))

        elif ext == '.xlsx':
            if 'openpyxl' not in sys.modules:
                print("❌ 缺少 openpyxl 库。请在 GitHub Actions 中配置 pip install openpyxl")
                return
            
            print(f"📄 正在解析 Excel 文件...")
            wb = openpyxl.load_workbook(file_name, data_only=True)
            sheet = wb.active

            uid_col_idx = None
            for col_idx, cell in enumerate(sheet[1], start=1):
                if cell.value and str(cell.value).strip().lower() == 'uid':
                    uid_col_idx = col_idx
                    break
            
            if not uid_col_idx:
                print("❌ Excel 文件的【第一行】表头里没有找到 'uid' 或 'UID' 列！请检查。")
                return

            for row_idx in range(2, sheet.max_row + 1):
                val = sheet.cell(row=row_idx, column=uid_col_idx).value
                if val is not None:
                    val_str = str(val).strip()
                    if val_str.isdigit():
                        target_uids.append(int(val_str))
        else:
            print(f"❌ 不支持的文件格式: {ext}，仅支持 .csv 或 .xlsx")
            return

    except Exception as e:
        print(f"❌ 读取文件失败: {e}")
        traceback.print_exc()
        return

    target_uids = list(set(target_uids))

    if not target_uids:
        print("⚠️ 文件里没有解析到任何有效的 UID。")
        return
        
    print(f"✅ 成功从 {ext.upper()} 载入 {len(target_uids)} 个有效 UID。")
    print("正在数据库中执行匹配分析，请稍候...")

    pipeline = [
        {"$match": {"uid": {"$in": target_uids}}},
        {"$group": {
            "_id": None,
            "totalUsers": {"$sum": 1},
            "activeUsers": {
                "$sum": {"$cond": [{"$and": [{"$gte": ["$updatedAt", start_utc]}, {"$lt": ["$updatedAt", end_utc]}]}, 1, 0]}
            },
            "rechargeUsers": {
                "$sum": {"$cond": [{"$and": [{"$ne": ["$meta.lastRechargeAt", None]}, {"$gte": ["$meta.lastRechargeAt", start_utc]}, {"$lt": ["$meta.lastRechargeAt", end_utc]}]}, 1, 0]}
            }
        }},
        {"$project": {
            "_id": 0,
            "totalUsers": 1,
            "activeUsers": 1,
            "rechargeUsers": 1,
            "rechargeRate": {
                "$cond": [{"$eq": ["$activeUsers", 0]}, 0, {"$divide": ["$rechargeUsers", "$activeUsers"]}]
            }
        }}
    ]

    result = list(db["users"].aggregate(pipeline, allowDiskUse=True))
    
    print("\n" + "🌟"*20)
    print(f"  SMS 召回数据统计 ({date_str})")
    print("🌟"*20)
    if result:
        data = result[0]
        active = data.get('activeUsers', 0)
        recharge = data.get('rechargeUsers', 0)
        rate = data.get('rechargeRate', 0)
        
        print(f"🔹 目标用户总数: {data.get('totalUsers', 0):,} 人")
        print(f"🔹 区间活跃用户: {active:,} 人")
        print(f"🔹 区间充值用户: {recharge:,} 人")
        print(f"🔹 充值转化率  : {rate:.2%} ({rate})")
    else:
        print("⚠️ 没有查询到结果 (可能文件中的 UID 在库中均不存在)")
    print("🌟"*20)

def run_report_4_unrecharged_users(db, config, end_utc, end_date_str):
    output_file = f"注册未充值用户_{config['db_name']}_{end_date_str}.csv"
    print(f"\n[1/2] 正在筛选注册未充值用户 (截止到 {end_date_str} 23:59:59)...")

    query = {
        "role": {"$ne": "gm"},
        "rechargeCount": 0,
        "updatedAt": {"$lt": end_utc}
    }
    
    projection = {
        "_id": 0,
        "uid": 1,
        "phone": 1,
        "email": 1,
        "updatedAt": 1
    }

    print(f"  执行数据库检索 (分批拉取数据)，请稍候...")
    
    cursor = db["users"].find(query, projection, batch_size=5000)

    print(f"[2/2] 正在生成导出文件...")
    count = 0
    batch_data = []
    batch_size = 10000

    with open(output_file, 'w', newline='', encoding='utf-8-sig') as f:
        writer = csv.writer(f)
        writer.writerow(["uid", "手机号", "邮箱", "最后活跃时间(东八区)"])

        for doc in cursor:
            phone = doc.get('phone', '') or ''
            email = doc.get('email', '') or ''
            
            batch_data.append([
                doc.get('uid', ''),
                phone,
                email,
                safe_date_format(doc.get('updatedAt'))
            ])
            count += 1
            
            if count % batch_size == 0:
                writer.writerows(batch_data)
                batch_data = []
                print(f"  已处理并导出 {count} 条用户数据...")

        if batch_data:
            writer.writerows(batch_data)

    print(f"✅ 导出成功！共计 {count} 名注册未充值用户。")
    print(f"文件位置: {os.path.abspath(output_file)}")

def main():
    print("=" * 50)
    print(f"      运营数据自动化导出工具 v{VERSION}")
    print("=" * 50)

    try:
        config = load_or_ask_config()

        if not config["mongo_uri"] or not config["db_name"] or not config["app_id"]:
            print("❌ 核心配置不完整，程序退出。")
            return

        print("\n--- 请选择要执行的功能 ---")
        print("[1] 导出 - 充提数据")
        print("[2] 导出 - 首存订单")
        print("[3] 打印 - 书生计算SMS召回情况 (支持 .csv / .xlsx)")
        print("[4] 导出 - 书生筛选注册未充值用户 (用于拉新/激活短信)")
        
        choice = input(">> ").strip()

        print("\n--- 日期范围设置 ---")
        start_in = input("请输入起始日期 (YYYY-MM-DD, 回车默认昨天): ").strip()
        
        if not start_in:
            start_date = datetime.now() - timedelta(days=1)
            start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
        else:
            try:
                start_date = datetime.strptime(start_in, "%Y-%m-%d")
            except ValueError:
                print("❌ 日期格式错误，请使用 YYYY-MM-DD")
                return

        end_in = input(f"请输入结束日期 (YYYY-MM-DD, 回车默认与起始日期相同 [{start_date.strftime('%Y-%m-%d')}]): ").strip()
        if not end_in:
            end_date = start_date
        else:
            try:
                end_date = datetime.strptime(end_in, "%Y-%m-%d")
            except ValueError:
                print("❌ 日期格式错误，请使用 YYYY-MM-DD")
                return
        
        if end_date < start_date:
            print("❌ 结束日期不能早于起始日期！")
            return

        start_utc = start_date - timedelta(hours=8)
        end_utc = end_date + timedelta(days=1) - timedelta(hours=8)
        
        if start_date == end_date:
            date_str = start_date.strftime("%Y-%m-%d")
        else:
            date_str = f"{start_date.strftime('%Y-%m-%d')}_至_{end_date.strftime('%Y-%m-%d')}"
        
        if choice == '4':
            print(f"\n⏳ 统计截止时间 (北京时间): {end_date.strftime('%Y-%m-%d 23:59:59')}")
        else:
            print(f"\n⏳ 统计时间范围 (北京时间): {start_date.strftime('%Y-%m-%d 00:00:00')} -> {end_date.strftime('%Y-%m-%d 23:59:59')}")

        print("\n正在连接 MongoDB...")
        client = pymongo.MongoClient(config['mongo_uri'], serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        db = client[config['db_name']]
        print("✅ MongoDB 连接成功")

        if choice == '1': 
            run_report_1_chongti(db, config, start_utc, end_utc, date_str)
        elif choice == '2': 
            run_report_2_shoucun(db, config, start_utc, end_utc, date_str)
        elif choice == '3': 
            run_report_3_sms_recall(db, config, start_utc, end_utc, date_str)
        elif choice == '4': 
            end_date_str = end_date.strftime('%Y-%m-%d')
            run_report_4_unrecharged_users(db, config, end_utc, end_date_str)
        else: 
            print("❌ 无效选择")

    except pymongo.errors.ServerSelectionTimeoutError:
        print("\n❌ MongoDB 连接失败 (连接超时)")
        print("请检查：")
        print("1. MongoDB 地址是否正确")
        print("2. 是否连接公司网络/VPN")
        print("3. 当前 IP 是否在数据库白名单中")

    except pymongo.errors.OperationFailure:
        print("\n❌ MongoDB 鉴权失败")
        print("请检查用户名、密码、数据库权限是否正确")

    except pymongo.errors.ConfigurationError:
        print("\n❌ MongoDB 配置错误")
        print("请检查 MongoDB URI 格式是否以 mongodb:// 开头")

    except KeyboardInterrupt:
        print("\n\n⚠️ 用户已手动取消操作")

    except Exception as e:
        print("\n❌ 程序运行发生未知错误")
        traceback.print_exc()

    finally:
        print("\n" + "=" * 50)
        input("程序已结束，按 [回车键] 关闭窗口...")

if __name__ == "__main__":
    main()
