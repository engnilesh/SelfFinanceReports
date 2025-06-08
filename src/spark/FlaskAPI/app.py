# example_app.py
from flask import Flask, jsonify
import subprocess
from datetime import datetime
import pytz

app = Flask(__name__)

@app.route('/trasform-and-load-mails-from-json-file-to-hive-jy-nb', methods=['POST'])
def trasform_and_load_mails_from_json_file_to_hive():
    try:
        ist = pytz.timezone('Asia/Kolkata')
        now_ist = datetime.now(ist)
        
        TS = now_ist.strftime("%Y-%m-%d-%H:%M:%S")
        cmd = f"jupyter nbconvert --to html --execute --output-dir /home/sparkuser/app/Read_Write_HDFS-Hive/jupyter_nb_ran_from_shell_output_dir/trasform_and_load_mails_from_json_file_to_hive_{TS} /home/sparkuser/app/Read_Write_HDFS-Hive/trasform_and_load_mails_from_json_file_to_hive.ipynb"
        # Run your Python script logic here or call a subprocess
        result = subprocess.run(cmd, capture_output=True, text=True, shell=True)
        return jsonify({"status": "success", "output": result.stdout})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/update-current-watermark-in-metadata-table-jy-nb', methods=['POST'])
def update_current_watermark_in_metadata_table():
    try:
        ist = pytz.timezone('Asia/Kolkata')
        now_ist = datetime.now(ist)
        
        TS = now_ist.strftime("%Y-%m-%d-%H:%M:%S")
        cmd = f"jupyter nbconvert --to html --execute --output-dir /home/sparkuser/app/Read_Write_HDFS-Hive/jupyter_nb_ran_from_shell_output_dir/update-current-watermark-in-metadata-table_{TS} /home/sparkuser/app/Read_Write_HDFS-Hive/update-current-watermark-in-metadata-table.ipynb"
        # Run your Python script logic here or call a subprocess
        result = subprocess.run(cmd, capture_output=True, text=True, shell=True)
        return jsonify({"status": "success", "output": result.stdout})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)



