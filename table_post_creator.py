import spanner_conn

table_name_post_creator = "PostCreator"

column_user_type = "userType"
column_uid = "uid"


def add_influencer_with_type(uid_arr, user_type):
    value_arr = []
    for uid in uid_arr:
        value_arr.append((uid, user_type))
    return spanner_conn.batchInsertOrUpdate(table_name_post_creator, [column_uid, column_user_type], value_arr)


def delete_inluencer(uid_arr):
    return spanner_conn.batchDelete(table_name_post_creator, uid_arr)

def view_users(user_type):
    return spanner_conn.get_priority_users(table_name_post_creator, user_type)
