import subprocess
import os
from google.cloud import storage
from google.cloud import speech_v1 as speech
import logging
import time

def process_audio(event, context):
    try:
        # Debug the incoming event
        print(f"Event payload: {event}")
        language_code = "de-DE"  # Default language code

        # Correctly extract bucket and file name from the event payload
        bucket_name = event['bucket']
        file_name = event['name']

        # Validate bucket and file name
        if not bucket_name or not file_name:
            raise ValueError(f"Missing bucket or file name in event payload: {event}")

        print(f"Processing file: {bucket_name}/{file_name}")

        # Determine file extension
        file_extension = file_name.split(".")[-1].lower()
        if file_extension == "wav" and "_converted" in file_name:
            print(f"Skipping already processed converted file: {file_name}")
            return
        # Initialize Storage Client
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)

        # Download file locally
        local_file = f"/tmp/{file_name}"
        print(f"Downloading file to: {local_file}")
        blob.download_to_filename(local_file)

        # If the file is already a .wav, proceed directly to transcription
        if file_extension == "wav":
            print("File is already a .wav, proceeding with transcription...")
            gcs_uri = f"gs://{bucket_name}/{file_name}"
            transcription = transcribe_audio(gcs_uri, file_extension, language_code)

            # Save transcription to GCS
            transcription_blob_name = f"{file_name}.txt"
            transcription_blob = bucket.blob(transcription_blob_name)
            transcription_blob.upload_from_string(transcription, content_type="text/plain")
            print(f"Transcription saved to {bucket_name}/{transcription_blob_name}")
            return

        # Convert .m4a to .wav if necessary
        if file_extension == "m4a":
            print("Converting .m4a to .wav")
            local_converted = f"/tmp/{file_name}_converted.wav"  # Append '_converted'
            try:
                subprocess.run([
                    "ffmpeg", "-y", "-i", local_file, "-acodec", "pcm_s16le", "-ar", "16000", local_converted
                ], check=True)
            except subprocess.CalledProcessError as e:
                print(f"ffmpeg failed with error: {e}")
                raise

            # Delete original file to free up space
            if os.path.exists(local_file):
                os.remove(local_file)

            # Upload converted .wav back to GCS
            print(f"Uploading converted file: {local_converted}")
            converted_blob_name = f"{file_name}_converted.wav"
            converted_blob = bucket.blob(converted_blob_name)
            converted_blob.upload_from_filename(local_converted)

            # Update file extension and URI for transcription
            file_extension = "wav"
            gcs_uri = f"gs://{bucket_name}/{converted_blob_name}"
            # Cleanup temporary files
            if os.path.exists(local_converted):
                os.remove(local_converted)
                
        elif file_extension == "mp3":
            # Use the MP3 file directly
            print(f"File is MP3, proceeding with transcription...")
            gcs_uri = f"gs://{bucket_name}/{file_name}"
        else:
            raise ValueError(f"Unsupported file format: {file_extension}")

        # Transcribe the audio file
        print("Starting transcription...")
        transcription = transcribe_audio(gcs_uri, file_extension, language_code)

        # Save transcription to GCS
        transcription_blob_name = f"{file_name}.txt"
        transcription_blob = bucket.blob(transcription_blob_name)
        transcription_blob.upload_from_string(transcription, content_type="text/plain")
        print(f"Transcription saved to {bucket_name}/{transcription_blob_name}")

    except Exception as e:
        print(f"Error processing file: {e}")


def transcribe_audio(gcs_uri, file_extension, language_code):
    """Transcribe audio using Google Cloud Speech-to-Text."""
    try:
        logging.basicConfig(level=logging.DEBUG)
        print(f"Starting transcription for: {gcs_uri}")
        client = speech.SpeechClient()
        print(f"Transcribing audio file: {gcs_uri}")
        # Set encoding based on file format
        encoding = speech.RecognitionConfig.AudioEncoding.LINEAR16 if file_extension == "wav" else speech.RecognitionConfig.AudioEncoding.MP3
        print(f"Using encoding: {encoding}")
        config = speech.RecognitionConfig(
            encoding=encoding,
            sample_rate_hertz=16000,  # Adjust if the audio sample rate is different
            language_code=language_code,
            enable_automatic_punctuation=True,
        )
        print(f"Transcription config: {config}")
        audio = speech.RecognitionAudio(uri=gcs_uri)
        print(f"Transcription audio: {audio}")
        # Use long-running operation for large files
        operation = client.long_running_recognize(config=config, audio=audio)
        print("Waiting for transcription to complete...")


        while not operation.done():
            metadata = operation.metadata  # Access operation metadata
            if metadata and hasattr(metadata, "progress_percent"):
                print(f"Progress: {metadata.progress_percent}%")
            else:
                print("Waiting for transcription progress update...")
            time.sleep(5)  # Wait for a few seconds

        response = operation.result(timeout=1000)
        print("Transcription operation completed.")

        # Compile transcription
        transcription = "\n".join([result.alternatives[0].transcript for result in response.results])
        print(f"Transcription: {transcription[:100]}...")  # Print only first 100 characters for debugging
        return transcription

    except Exception as e:
        print(f"Error during transcription: {e}")
        raise
