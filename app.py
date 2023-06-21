"""
This is a main function for the application.
Version : 2.0
Author: Biswajit Mohapatra, Swati Kahar
"""

# Importing all the dependencies...
import os
import pandas as pd
import pickle as pkl
from flask import Flask, request, render_template
from flask_cors import cross_origin
from sqlalchemy import null
from pretty_html_table import build_table
from src.question_duplicate_package import duplicate_v1
from src import data_prep,cloud_operations 
from src.utils.common import read_yaml,create_directories,delete_file
from application_logger.logging import App_Logger

app = Flask(__name__)
file_obj =open("MAIN.txt","a+")
log = App_Logger(file_obj)

@app.route("/")
@cross_origin()
def home():
    """
    This function is responsible for rendering the home page.
    """
    return render_template("home.html")

@app.route("/duplicate", methods = ["GET", "POST"])
@cross_origin()
def main():
    """
    This function is responsible for rendering the number of duplicate
    along with schoolID.
    Condition: The question that needed to be checked should be provided along with the school ID
    else the home page will be rendered by default.
    """
    if request.method == "POST":
        try:
            log.log("Entered main function and getting the credentials from the config file.")
            scentence = request.form.get("Question")
            school_code = request.form.get("SchoolCode")
            scentence = scentence.strip().lower()
            config = read_yaml("config.yaml")
            local_dir = config["GET_DATA"]["local_dir"]
            auth_path = config["GET_DATA"]["auth_json_path"]
            transformed_data_file = config["GET_DATA"]["transformed_data_file"]
            school_code_list = config["GET_DATA"]["school_codes"]
            bucket_auth = config["GET_DATA"]["bucket_auth"]
            bucket_name = config["GET_DATA"]["bucket_name"]
            file_name = f"{school_code}_{transformed_data_file}"

            # defining blob name for storing in cloud.
            blob_name = file_name 

            # file path to pass for cloud operations.
            upload_file_path = os.path.join(local_dir, file_name) 
            cloud_ops = cloud_operations.cloud_ops(bucket_auth,blob_name,bucket_name,upload_file_path)
            fileNames_recived = cloud_ops.check_for_blob_presence()
            
            # calling the question duplicate package.
            duplicate = duplicate_v1(auth_path)
            if not blob_name in fileNames_recived:
                log.log(f"Blob with name :: {blob_name} isn't present, so fetching the data from Big-query.")
                # defining path for transformed_data_file:

                # Fetching the data
                if not os.path.exists(local_dir):
                    log.log(log_message=f"creating data directory...")
                    create_directories([local_dir])
                log.log(f"Connecting to the Big Query...")

                # Connecting to Big Query
                duplicate.connect_bigquerry() 
                log.log(f"Connected to the Big Query...")
                log.log(f"Getting the data...")

                # Downloading the data from Big Query if data not present
                if not os.path.exists(upload_file_path):
                    data =  duplicate.fetch_data(school_code=school_code,school_code_list=list(school_code_list)) 
                else:
                    with open(upload_file_path, "rb") as f:
                        object = pkl.load(f)
                    data = pd.DataFrame(object) # reading the saved pickled data
                log.log(f"Data getting completed...")

        
                # transforming the cleaned data
                if not os.path.exists(upload_file_path):
                    # Preparing the data:
                    prepared_data = data_prep.prepare_data(data)
            
                    # Cleaning the data:
                    log.log("Instantiating claning of the data...")
                    data_cleaned = duplicate.clean_data(prepared_data)# removing the html tags from the data and storing the data
                    column_name = "clean_question_data"
                    data_cleaned_nan = duplicate.clean_nan(data_cleaned,column_name)# remove the nan created while removing the html tags
                    log.log(f"Data cleaning completed...")

                    #inserting column to the data:
                    col_name = "cleaned_mcq_questions_options"
                    duplicate.insert_col(data_cleaned_nan,18,col_name,value="")
                    log.log(f"{col_name} successfully added.")
                    transformed_data = duplicate.transform_data(data_cleaned_nan)
                    log.log(f"Data tranformation completed, now saving the file to pickle.")

                    # saving the transformed data
                    transformed_data.to_pickle(upload_file_path) 
                    log.log(f"Data saved successfully at :: {upload_file_path}")

                    # uploading the saved data to google service.
                    log.log(f"Uploading the data to the google cloud storage.")
                    blob = cloud_ops.upload_file()
                    log.log(f"File uploaded successfully to the google cloude service with blob name as :: {blob.id}")

                    # Deleting the created file from local memory after uploading.
                    if os.path.exists(upload_file_path):
                        log.log(f"Deleting the file after uploading to the google storage :: {upload_file_path}")
                        delete_file(upload_file_path)
                        log.log(f"Deleted the file after uploading to the google storage :: {upload_file_path}")
                else:
                    with open(upload_file_path, "rb") as f:
                        object = pkl.load(f)
                    transformed_data = pd.DataFrame(object)

            else:
                log.log(f"Blob with {blob_name} present in the cloud storage, so staring the download.")
                # downloading the file
                cloud_ops.download_file(upload_file_path) 
                log.log(f"Successfully downloaded the file.")
                with open(upload_file_path, "rb") as f:
                        object = pkl.load(f)
                transformed_data = pd.DataFrame(object)

            log.log(f"Starting Data Filteration \n")
            # Filtering out the data whose duplicates exists:
            filtered_data = duplicate.filter_duplicate(transformed_data)

            # deleting the file after downloading.
            if os.path.exists(upload_file_path):
                log.log(f"Data fetched successfully so deleting the downloaded file :: {upload_file_path}")
                delete_file(upload_file_path)
                log.log(f"Data fetched successfully so deleted the downloaded file :: {upload_file_path}")
            log.log(f"Data filteration completed...\n")

            #If the secentence is present, the following operation will be performed.
            if scentence:
                log.log(f"Finding duplicate index started...\n")
                idx = duplicate.find_dup_idx(filtered_data,scentence)
                template = "templates"
                file_name_html = "details.html"
                path = os.path.join(template, file_name_html)
                if len(idx)>1:
                    dup = duplicate.variations(filtered_data=filtered_data,idx=idx[1::])
                    new_data = duplicate.fetch_duplicate_data(dup)
                    original_path = os.getcwd()
                    if not os.path.exists(path):
                        log.log(f"{file_name_html} isn't present so saving it...")
                        os.chdir(template)
                        html_table = build_table(new_data, 'blue_light')
                        with open(file_name_html, 'w',encoding="utf-8") as f:
                            f.write(html_table)
                        os.chdir(original_path)
                        log.log(f"{file_name_html} saved successfully at {path}")
                    else:
                        log.log(f"creating new file at {path}...")
                        os.chdir(template)
                        delete_file(file_name_html)
                        html_table = build_table(new_data, 'blue_light')
                        with open(file_name_html, 'w',encoding="utf-8") as f:
                            f.write(html_table)
                        os.chdir(original_path)
                        log.log(f"New file created successfully at {path}")
                    return render_template('home.html',prediction_output = f"Number of Duplicates found :: {len(idx)-1}")
                else:
                    log.log(f"{file_name_html} already exists so deleting it...")
                    if os.path.exists(path):
                        delete_file(path)
                    return render_template('home.html',prediction_output = f"Number of Duplicates found :: 0")


        except KeyError as k:
            log.log(f"Something went wrong :: {k}\n")
            raise KeyError
        except AttributeError as a:
            log.log(f"Something went wrong :: {a}\n")
            raise AttributeError
        except ValueError as v:
            log.log(f"Something went wrong :: {v}\n")
            raise ValueError
        except Exception as e:
            log.log(f"Something went wrong :: {e}\n")
            raise Exception

    else:
        return render_template('home.html',prediction_output = "No values were added...")

@app.route("/details", methods=["GET", "POST"])
@cross_origin()
def details():
    """
    This function is responsible for rendering the details of duplicate
    question on a different page.
    Condition: The file must be present before rendering
    else the home page will only be rendered. 
    """
    if request.method == "POST":
        log.log(f"Request method recived for details api...")
        path = "templates/details.html"
        if os.path.exists(path):
            log.log(f"File exists, hence rendering..")
            return render_template('details.html')
        else:
            log.log(f"File does not exist, hence rendering home page...")
            return render_template('home.html',output="No details present for 0 matches.")
    else:
        log.log(f"Request method not recived, hence rendering home.html")
        return render_template('home.html',output="No details present for 0 matches.")

if __name__ == "__main__":
    app.run(host='0.0.0.0',port=5000)