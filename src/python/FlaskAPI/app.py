# example_app.py
from flask import Flask, jsonify
import subprocess
from datetime import datetime
import pytz
import papermill as pm
import json
import os

app = Flask(__name__)

@app.route('/hello-world', methods=['POST'])
def hello_world():
    ist = pytz.timezone('Asia/Kolkata')
    now_ist = datetime.now(ist)
    
    TS = now_ist.strftime("%Y-%m-%d-%H:%M:%S")
    try:
        # Run your Python script logic here or call a subprocess
        result = subprocess.run(['python', 'script.py'], capture_output=True, text=True)
        return jsonify({"status": "success", "output": result.stdout})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/sample-jy-script', methods=['POST'])
def sample_jy_script():
    ist = pytz.timezone('Asia/Kolkata')
    now_ist = datetime.now(ist)
    
    TS = now_ist.strftime("%Y-%m-%d-%H:%M:%S")
    try:
        cmd = f"jupyter nbconvert --to html --execute --output-dir ~/code/ext_code/fetch_mail_using_imap_lib/jupyter_nb_ran_from_shell_output_dir/sample_notebook_{TS} ~/code/ext_code/fetch_mail_using_imap_lib/sample_notebook.ipynb"
        # Run your Python script logic here or call a subprocess
        result = subprocess.run(cmd, capture_output=True, text=True, shell=True)
        return jsonify({"status": "success", "output": result.stdout})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/extract-mails-into-json-file-jy-nb', methods=['POST'])
def extract_mails_into_json_file():
    ist = pytz.timezone('Asia/Kolkata')
    now_ist = datetime.now(ist)
    
    TS = now_ist.strftime("%Y-%m-%d-%H:%M:%S")
    try:
        try:
            cmd = f"mkdir -p /home/commonid/code/ext_code/fetch_mail_using_imap_lib/jupyter_nb_ran_from_shell_output_dir/extract_mails_into_json_file_{TS}"
            result = subprocess.run(cmd, capture_output=True, text=True, shell=True)
        except Exception as e:
            print("Output directory not created, ERROR:", str(e))
        out_file = f'/home/commonid/code/ext_code/fetch_mail_using_imap_lib/jupyter_nb_ran_from_shell_output_dir/extract_mails_into_json_file_{TS}/extract_mails_into_json_file.ipynb'
        subprocess.run(
            ['/home/commonid/.local/bin/papermill', '/home/commonid/code/ext_code/fetch_mail_using_imap_lib/extract_mails_into_json_file.ipynb', out_file],
            check=True
        )
        return jsonify({"status": "success", "output": f"please see the {out_file} for more details"})
    except subprocess.CalledProcessError as e:
        exitcode_json_file = "/home/commonid/code/ext_code/fetch_mail_using_imap_lib/exit_status_code.json"
        with open(exitcode_json_file, 'r') as file:
            content = json.load(file)
        if content["exitcode"] == 10:
            os.remove(exitcode_json_file)
            return jsonify({"status": "error", "message": content["Message"]}), 404
        else:
            return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)


