from flask import Flask, request, render_template
from flask import jsonify
import traceback
import json

from log import Logger
from status_url_api.status_thrift.TimelineService import Client
from status_url_api import spanner_conn
from status_url_api import google_storage_utils

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

logger = Logger.get_logger(__file__)

app = Flask(__name__)


@app.route('/get_status_url', methods=['POST'])
def get_url_for_posts():
    if request.method == 'POST':
        is_success = False
        status = 500
        data = []
        try:
            logger.debug("Request %s", request.get_json())
            post_arr = request.get_json().get("posts")

            transport.open()
            post_data_dict = client.getStatus(post_arr, False, True, False)
            transport.close()

            for post_id in post_data_dict.keys():
                post_data = json.loads(post_data_dict[post_id])
                post = dict(post_id=post_id,
                            post_url=post_data.get("full_url", None),
                            post_type=post_data.get("metadata", {}).get("asset", {}).get("type", None))
                data.append(post)
            is_success = True
            status = 200
        except Thrift.TException, tx:
            logger.error("Error in thrift client, %s", str(tx))
        except Exception as e:
            traceback.print_exc()
            logger.error("Error in adding data, %s", e)

        res = dict(is_success=is_success, data=data, status=status)
        return jsonify(**{'res': res})


@app.route('/feed/<string:page_name>/')
def get_rendered_page(page_name):
    try:
        return render_template('%s.html' % page_name)
    except Exception as e:
        traceback.print_exc()
        logger.error("Error in rendering page, %s", e)


@app.route('/get_post_html', methods=['POST'])
def get_html_for_posts():
    if request.method == 'POST':
        is_success = False
        status = 500
        data = []
        try:
            logger.debug("Request %s", request.get_json())
            post_arr = request.get_json().get("posts")

            query = "select post_id, fullUrl from post where post_id in (" + ",".join(
                '"' + post_id + '"' for post_id in post_arr) + ")"
            results = spanner_conn.executeQuery(query)


            file_name = "post_to_html"
            html_file = "status_url_api/templates/" + file_name + ".html"
            with open(html_file, 'w') as feed_file:
                feed_file.write("<!DOCTYPE html>\n")
                feed_file.write("<html>\n")
                feed_file.write("<body>\n")
                feed_file.write('<div style = "display: flex;flex-direction: row; flex-wrap: wrap">\n')

                for result in results:
                    post_id = result[0]
                    full_url = result[1]

                    if full_url is not None and full_url is not '':
                        feed_file.write(
                            '<div style = "display: flex;flex-direction: column;margin-left: 15px;margin-top: 15px">')
                        feed_file.write("<img height = 200 width = 200 src='" + full_url + "'/>\n")
                        feed_file.write("<text>" + post_id + "</text>\n")
                        feed_file.write('</div>\n')
                #             else:
                #                 print("Data not found", post_id)
                feed_file.write("</div>\n")
                feed_file.write("</body>\n")
                feed_file.write("</html>")
                data.append("http://10.9.51.33:8081/feed/" + file_name)
            is_success = True
            status = 200
        except Exception as e:
            traceback.print_exc()
            logger.error("Error in adding data, %s", e)
        res = dict(is_success=is_success, data=data, status=status)
        return jsonify(**{'res': res})

@app.route('/get_similar_post', methods=['POST'])
def get_similar_posts():
    if request.method == 'POST':
        is_success = False
        status = 500
        data = []
        try:
            logger.debug("Request %s", request.get_json())
            bucket = request.get_json().get("gs_bucket")
            csv_location = request.get_json().get("csv_location")
            similar_img_cnt = request.get_json().get("similar_img_cnt", default=5)

            if csv_location is not None and csv_location is not "":
                google_storage_utils.download_file_from_gs(bucket, csv_location, "temp.csv")
                is_success = True
                status = 200
            else:
                data = ["NO csv location given"]
        except Exception as e:
            traceback.print_exc()
            logger.error("Error in adding data, %s", e)
        res = dict(is_success=is_success, data=data, status=status)
        return jsonify(**{'res': res})

if __name__ == '__main__':
    print "starting status_thrift client..."
    transport = TSocket.TSocket('timeline.service.hike.in', 9095)
    transport = TTransport.TBufferedTransport(transport)
    protocol = TBinaryProtocol.TBinaryProtocol(transport)
    client = Client(protocol)
    app.run(host='0.0.0.0', port=8081, threaded=True, debug=True)
