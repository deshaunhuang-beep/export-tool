import csv
import pymongo
import pymongo.errors
from datetime import datetime, timedelta
import os
import json

CONFIG_FILE = "config.json"
VERSION = "1.0.0"


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

    mongo_in = input(
        f"MongoDB 地址{' (回车保持现状)' if config['mongo_uri'] else ''}: "
    ).strip()

    if mongo_in:
        config["mongo_uri"] = mongo_in

    db_in = input(
        f"数据库名称{' (回车保持现状)' if config['db_name'] else ''}: "
    ).strip()

    if db_in:
        config["db_name"] = db_in

    app_in = input(
        f"业务 AppID{' (回车保持现状)' if config['app_id'] else ''}: "
    ).strip()

    if app_in:
        try:
            config["app_id"] = int(app_in)
        except ValueError:
            print("❌ AppID 必须是数字")
            return load_or_ask_config()

    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(config, f, indent=4, ensure_ascii=False)

    return config


def run_report_1_chongti(db, config, start_utc, end_utc, date_str):
    output_file = f"充提数据_{date_str}.csv"

    print(f"\n[1/4] 正在拉取基础订单...")

    pipeline = [
        {
            "$match": {
                "appID": config['app_id'],
                "updatedAt": {
                    "$gte": start_utc,
                    "$lt": end_utc
                },
                "type": {
                    "$in": ['pay', 'withdrawal']
                },
                "status": {
                    "$in": ['Completed', 'MockCompleted']
                },
                "ignoreAnalysis": {
                    "$ne": True
                }
            }
        },
        {
            "$group": {
                "_id": {
                    "user": "$user",
                    "channel": {
                        "$ifNull": ["$channel", 0]
                    }
                },
                "firstStatus": {
                    "$first": "$status"
                },
                "存款次数": {
                    "$sum": {
                        "$cond": [
                            {"$eq": ["$type", "pay"]},
                            1,
                            0
                        ]
                    }
                },
                "存款金额": {
                    "$sum": {
                        "$cond": [
                            {"$eq": ["$type", "pay"]},
                            {"$ifNull": ["$totalPrice", 0]},
                            0
                        ]
                    }
                },
                "提款金额": {
                    "$sum": {
                        "$cond": [
                            {"$eq": ["$type", "withdrawal"]},
                            {"$ifNull": ["$totalPrice", 0]},
                            0
                        ]
                    }
                }
            }
        }
    ]

    order_results = list(
        db["orders"].aggregate(
            pipeline,
            allowDiskUse=True
        )
    )

    if not order_results:
        print("⚠️ 未找到数据")
        return

    uids = [res['_id']['user'] for res in order_results]

    print(f"[2/4] 同步 {len(uids)} 个用户数据...")

    user_map = {
        u['_id']: u
        for u in db["users"].find({
            "_id": {"$in": uids}
        })
    }

    daily_map = {
        d['user']: d
        for d in db["transactiondailies"].find({
            "user": {"$in": uids},
            "startAt": start_utc
        })
    }

    print(f"[3/4] 写入文件...")

    with open(output_file, 'w', newline='', encoding='utf-8-sig') as f:
        writer = csv.writer(f)

        writer.writerow([
            "用户ID",
            "用户渠道",
            "注册日期",
            "是否模拟回调",
            "存款金额",
            "提款金额",
            "当日存款次数",
            "当日真金奖励",
            "当日投注金额"
        ])

        for doc in order_results:
            u_id = doc['_id']['user']

            u = user_map.get(u_id, {})
            d = daily_map.get(u_id, {})

            writer.writerow([
                u.get('uid', ''),
                "广告" if u.get('meta', {}).get('adChannel') else "自然裂变",
                safe_date_format(u.get('createdAt')),
                "是" if doc.get('firstStatus') == 'MockCompleted' else "否",
                doc.get('存款金额', 0),
                doc.get('提款金额', 0),
                doc.get('存款次数', 0),
                d.get('rewardCash', 0),
                d.get('betAmount', 0)
            ])

    print(f"✅ 完成: {output_file}")


def run_report_2_shoucun(db, config, start_utc, end_utc, date_str):
    output_file = f"首存订单_{date_str}.csv"

    print(f"\n[1/4] 正在统计今日充值用户...")

    pay_pipeline = [
        {
            "$match": {
                "appID": config['app_id'],
                "updatedAt": {
                    "$gte": start_utc,
                    "$lt": end_utc
                },
                "type": "pay",
                "status": "Completed",
                "ignoreAnalysis": {
                    "$ne": True
                }
            }
        },
        {
            "$sort": {
                "updatedAt": 1,
                "_id": 1
            }
        },
        {
            "$group": {
                "_id": "$user",
                "次数": {
                    "$sum": 1
                },
                "总额": {
                    "$sum": "$totalPrice"
                },
                "第一笔": {
                    "$first": "$totalPrice"
                }
            }
        }
    ]

    pay_results = list(
        db["orders"].aggregate(
            pay_pipeline,
            allowDiskUse=True
        )
    )

    uids = [res['_id'] for res in pay_results]

    print(f"[2/4] 正在筛选真正的首存用户...")

    shoucun_users = {
        u['_id']: u
        for u in db["users"].find({
            "_id": {"$in": uids},
            "meta.firstRechargeAt": {
                "$gte": start_utc,
                "$lt": end_utc
            }
        })
    }

    sc_uids = list(shoucun_users.keys())

    if not sc_uids:
        print("⚠️ 今日无首存用户")
        return

    print(f"[3/4] 抓取提款与日报数据 (共 {len(sc_uids)} 人)...")

    wd_pipeline = [
        {
            "$match": {
                "user": {"$in": sc_uids},
                "type": "withdrawal",
                "status": "Completed",
                "updatedAt": {
                    "$gte": start_utc,
                    "$lt": end_utc
                }
            }
        },
        {
            "$group": {
                "_id": "$user",
                "total": {
                    "$sum": "$totalPrice"
                }
            }
        }
    ]

    wd_map = {
        res['_id']: res['total']
        for res in db["orders"].aggregate(wd_pipeline)
    }

    daily_map = {
        d['user']: d
        for d in db["transactiondailies"].find({
            "user": {"$in": sc_uids},
            "startAt": start_utc
        })
    }

    print(f"[4/4] 导出报表...")

    headers = [
        "用户渠道",
        "用户ID",
        "注册日期",
        "首次充值日期",
        "当天充值次数",
        "第一笔充值金额",
        "当天总充值金额",
        "提款金额",
        "当日所获得真金奖励",
        "当日投注金额"
    ]

    with open(output_file, 'w', newline='', encoding='utf-8-sig') as f:
        writer = csv.writer(f)

        writer.writerow(headers)

        for res in pay_results:
            uid = res['_id']

            if uid not in shoucun_users:
                continue

            u = shoucun_users[uid]
            d = daily_map.get(uid, {})

            writer.writerow([
                "广告" if u.get('meta', {}).get('adChannel') else "自然裂变",
                u.get('uid', ''),
                safe_date_format(u.get('createdAt')),
                safe_date_format(u.get('meta', {}).get('firstRechargeAt')),
                res.get('次数', 0),
                res.get('第一笔', 0),
                res.get('总额', 0),
                wd_map.get(uid, 0),
                d.get('rewardCash', 0),
                d.get('betAmount', 0)
            ])

    print(f"✅ 完成: {output_file}")


def main():
    print("=" * 50)
    print(f"运营数据自动化导出工具 v{VERSION}")
    print("=" * 50)

    try:
        config = load_or_ask_config()

        if not config["mongo_uri"]:
            print("❌ MongoDB 地址不能为空")
            return

        if not config["db_name"]:
            print("❌ 数据库名称不能为空")
            return

        if not config["app_id"]:
            print("❌ AppID 不能为空")
            return

        print("\n请选择类型:")
        print("[1] 充提数据")
        print("[2] 首存订单")

        choice = input(">> ").strip()

        date_in = input(
            "\n日期 (YYYY-MM-DD, 回车默认昨天): "
        ).strip()

        try:
            target_date = (
                datetime.strptime(date_in, "%Y-%m-%d")
                if date_in
                else datetime.now() - timedelta(days=1)
            )
        except ValueError:
            print("❌ 日期格式错误，请使用 YYYY-MM-DD")
            return

        start_utc = datetime(
            target_date.year,
            target_date.month,
            target_date.day
        ) - timedelta(hours=8)

        end_utc = start_utc + timedelta(days=1)

        date_str = target_date.strftime("%Y-%m-%d")

        print("\n正在连接 MongoDB...")

        client = pymongo.MongoClient(
            config['mongo_uri'],
            serverSelectionTimeoutMS=5000
        )

        client.admin.command('ping')

        db = client[config['db_name']]

        print("✅ MongoDB 连接成功")

        if choice == '1':
            run_report_1_chongti(
                db,
                config,
                start_utc,
                end_utc,
                date_str
            )

        elif choice == '2':
            run_report_2_shoucun(
                db,
                config,
                start_utc,
                end_utc,
                date_str
            )

        else:
            print("❌ 无效选择")

    except pymongo.errors.ServerSelectionTimeoutError:
        print("\n❌ MongoDB 连接失败")
        print("请检查：")
        print("1. MongoDB 地址是否正确")
        print("2. 是否连接公司网络/VPN")
        print("3. 当前 IP 是否在数据库白名单中")

    except pymongo.errors.OperationFailure:
        print("\n❌ MongoDB 鉴权失败")
        print("请检查用户名、密码、数据库权限是否正确")

    except pymongo.errors.ConfigurationError:
        print("\n❌ MongoDB 配置错误")
        print("请检查 MongoDB URI 格式")

    except KeyboardInterrupt:
        print("\n\n⚠️ 用户已取消操作")

    except Exception as e:
        print("\n❌ 程序运行失败")
        print(f"错误信息: {str(e)}")

    finally:
        print("\n" + "=" * 50)
        input("程序已结束，按 [回车键] 关闭窗口...")


if __name__ == "__main__":
    main()
