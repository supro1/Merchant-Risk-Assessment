from AzureBlobStorage.AzureBlobStorageManagement import AzureBlobManagement

your_connection_string="DefaultEndpointsProtocol=https;AccountName=waffer0;AccountKey=9hdZPGP0PAeSvzugNi5eisZ0GSTE/93gYY2BO0sPObfT9ZQqyXWQefmSgs79AF/nxlsEHaqQag2yhlhe5P08mQ==;EndpointSuffix=core.windows.net"


def initiateTransfer(your_connection_string):
    """

    :param your_connection_string: pass azure storage account connection string
    :return:
    """
    try:
        azm_source=AzureBlobManagement()
        azm_destination=AzureBlobManagement(your_connection_string)
        prediction_dir="prediction-batch-files"
        training_dir="training-batch-files"


        training_files=azm_source.getAllFileNameFromDirectory(training_dir)
        prediction_files=azm_source.getAllFileNameFromDirectory(prediction_dir)

        for training_file in training_files:
            df=azm_source.readCsvFileFromDirectory(training_dir,training_file)
            df.rename(columns={"Unnamed: 1": ""}, inplace=True)
            azm_destination.saveDataFrameTocsv(training_dir,training_file,df)
            print("File:{} transfered successfully to dir {} of account name {}".format(training_file,training_dir,azm_destination.blob_service_client.account_name))
        for prediction_file in prediction_files:
            df = azm_source.readCsvFileFromDirectory(prediction_dir, prediction_file)
            df.rename(columns={"Unnamed: 0.1": ""}, inplace=True)
            azm_destination.saveDataFrameTocsv(prediction_dir, prediction_file, df)
            print("File:{} transfered successfully to dir {} of account name {}".format(prediction_file,prediction_dir,azm_destination.blob_service_client.account_name))
    except Exception as e:
        print(str(e))


if __name__=="__main__":
    initiateTransfer(your_connection_string)






