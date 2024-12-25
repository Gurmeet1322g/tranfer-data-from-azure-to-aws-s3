"use strict";

/***********************************
 **** node module defined here *****
 ***********************************/
require("dotenv").config();
const express = require("express");
const {
  BlobServiceClient,
  StorageSharedKeyCredential,
} = require("@azure/storage-blob");
const AWS = require("aws-sdk");
const fs = require("fs");
const path = require("path");
const sgMail = require("@sendgrid/mail");
sgMail.setApiKey(process.env.SEDNGRID_API_KEY);
const app = express();
const port = 3000;

// Azure Blob Storage Configuration
const AZURE_STORAGE_CONNECTION_STRING =
  process.env.AZURE_STORAGE_CONNECTION_STRING;
const AZURE_CONTAINER_NAME = process.env.AZURE_CONTAINER_NAME;
const AZURE_BLOB_NAME = process.env.AZURE_BLOB_NAME;

// AWS S3 Configuration
const s3 = new AWS.S3({
  accessKeyId: process.env.accessKeyId,
  secretAccessKey: process.env.secretAccessKey,
  region: process.env.region,
});
const S3_BUCKET_NAME = process.env.S3_BUCKET_NAME;

app.use(async (req, res, next) => {
  try {
    // Step 1: Download the file from Azure Blob Storage
    const sharedKeyCredential = new StorageSharedKeyCredential(
      AZURE_CONTAINER_NAME,
      AZURE_STORAGE_CONNECTION_STRING
    );
    const blobServiceClient = new BlobServiceClient(
      `https://${AZURE_CONTAINER_NAME}.blob.core.windows.net`,
      sharedKeyCredential
    );
    const containerClient =
      blobServiceClient.getContainerClient(AZURE_BLOB_NAME);
    let pageCount = 1;

    for await (const response of containerClient
      .listBlobsFlat()
      .byPage({ maxPageSize: 10 })) {
      console.log(`Page ${pageCount}`);
      for (const blob of response.segment.blobItems) {
        console.log(` - ${blob.name}`);
        const tempFilePath = blob.name;
        const blobClient = containerClient.getBlobClient(tempFilePath);
        const downloadBlockBlobResponse = await blobClient.download(0);
        
        // Ensure directories exist
        const tempDir = path.dirname(tempFilePath);
        fs.mkdirSync(tempDir, { recursive: true });

        // Step 1: Download the file from Azure Blob Storage
        const writableStream = fs.createWriteStream(tempFilePath);
        await new Promise((resolve, reject) => {
          downloadBlockBlobResponse.readableStreamBody
            .pipe(writableStream)
            .on("finish", resolve)
            .on("error", reject);
        });
        console.log(`File downloaded to: ${tempFilePath}`);
        // Ensure directories exist
        const fileStream = fs.createReadStream(tempFilePath);
        // Step 2: Uploading files on s3 bucket
        const uploadParams = {
          Bucket: S3_BUCKET_NAME,
          Key: tempFilePath,
          Body: fileStream,
        };
        console.log(
          `Uploading file to AWS S3: ${S3_BUCKET_NAME}/${tempFilePath}`
        );
        await s3.upload(uploadParams).promise();
        console.log("File uploaded to AWS S3 successfully!");
        fs.unlinkSync(tempFilePath);
      }
      pageCount++;
    }
    // Step 3: Send email update after complete job
    await sgMail.send(
      {
        to: "gurmeet@headoffice.space", // Change to your recipient
        cc: "marco@headoffice.space",
        from: `Headoffice.ai<${process.env.SENDGRIND_EMAIL_SENDER}>`, // Change to your verified sender
        subject: "Success",
        html: `<h1>Your data successfully migrated on AWS s3 bucket : ${S3_BUCKET_NAME}</h1>`,
      },
      function (error, body) {
        if (error) {
          console.log(error);
        }
      }
    );
    console.log("Blob listing complete.");
    res.send("File transferred from Azure to AWS S3 successfully!");
  } catch (error) {
    console.error("Error transferring file:", error);
    res.status(500).send("An error occurred while transferring the file.");
  }
  next();
});

app.listen(port, () => {
  console.log(`Server running at http://localhost:${port}`);
});
