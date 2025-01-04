import { S3 } from "@aws-sdk/client-s3";
import { SFNClient, StartExecutionCommand } from "@aws-sdk/client-sfn";
import sqlite3 from 'sqlite3';
import { open } from 'sqlite';

const DB_PATH = '/mnt/efs/sqlite-data/customer_data.db';

async function initializeDatabase() {
    const db = await open({
        filename: DB_PATH,
        driver: sqlite3.Database,
    });

    await db.exec(`
        CREATE TABLE IF NOT EXISTS customers (
            customer_id INTEGER PRIMARY KEY,
            first_name TEXT,
            last_name TEXT,
            company TEXT,
            city TEXT,
            country TEXT,
            phone_1 TEXT,
            phone_2 TEXT,
            email TEXT,
            subscription_date TEXT,
            website TEXT
        )
    `);

    return db;
}

async function upsertCustomerData(db, customers) {
    const query = `
        INSERT INTO customers (
            customer_id, first_name, last_name, company, city, country, 
            phone_1, phone_2, email, subscription_date, website
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(customer_id) DO UPDATE SET
            first_name = excluded.first_name,
            last_name = excluded.last_name,
            company = excluded.company,
            city = excluded.city,
            country = excluded.country,
            phone_1 = excluded.phone_1,
            phone_2 = excluded.phone_2,
            email = excluded.email,
            subscription_date = excluded.subscription_date,
            website = excluded.website
    `;

    for (const customer of customers) {
        await db.run(query, [
            customer.customer_id,
            customer.first_name,
            customer.last_name,
            customer.company,
            customer.city,
            customer.country,
            customer.phone_1,
            customer.phone_2,
            customer.email,
            customer.subscription_date,
            customer.website,
        ]);
    }
}


const s3 = new S3();
const sfnClient = new SFNClient();

// Replace with your Step Functions state machine ARN
const stateMachineArn = "arn:aws:states:us-east-2:703671907972:stateMachine:ETL-Customer-Data-Pipeline";

export async function handler(event) {
    console.log("Received Event:", JSON.stringify(event, null, 2));

    let bucketName, objectKey;

    try {
        // Check the event structure (S3 or Step Functions payload)
        if (event.Records && event.Records[0]) {
            // S3 Event
            bucketName = event.Records[0].s3.bucket.name;
            objectKey = decodeURIComponent(event.Records[0].s3.object.key.replace(/\+/g, " "));
        } else if (event.bucketName && event.objectKey) {
            // Step Function Event
            bucketName = event.bucketName;
            objectKey = event.objectKey;
        } else {
            throw new Error("Unsupported event format");
        }

        console.log(`Processing file from bucket: ${bucketName}, key: ${objectKey}`);

        if (objectKey.startsWith("processed/")) {
            console.log(`File ${objectKey} is already in the processed/ folder. Skipping.`);
            return {
                statusCode: 200,
                body: JSON.stringify({ message: "File already processed. Skipping." }),
            };
        }

        // Fetch the file from S3
        const params = { Bucket: bucketName, Key: objectKey };
        const data = await s3.getObject(params);

        const streamToString = (stream) =>
            new Promise((resolve, reject) => {
                const chunks = [];
                stream.on("data", (chunk) => chunks.push(chunk));
                stream.on("error", reject);
                stream.on("end", () => resolve(Buffer.concat(chunks).toString("utf-8")));
            });

        const fileContent = await streamToString(data.Body);

        console.log("First 100 characters of the file:", fileContent.substring(0, 100));

        // Parse the CSV content
        const lines = fileContent.split("\n");
        const headers = lines[0].split(",");
        const firstNameIndex = headers.indexOf("First Name");

        if (firstNameIndex !== -1) {
            db = await initializeDatabase();
            for (let i = 1; i < lines.length; i++) {
                const columns = lines[i].split(",");
                const firstName = columns[firstNameIndex];
                if (firstName) console.log("First Name:", firstName);

                // Insert the data into the SQLite database
                const customers = [
                    {
                        customer_id: i,
                        first_name: columns[0],
                        last_name: columns[1],
                        company: columns[2],
                        city: columns[3],
                        country: columns[4],
                        phone_1: columns[5],
                        phone_2: columns[6],
                        email: columns[7],
                        subscription_date: columns[8],
                        website: columns[9],
                    },
                ];

                // Upsert data into the SQLite database
                await upsertCustomerData(db, parsedData);

                console.log("Customer data processed successfully.");
            }
        } else {
            console.log("First Name column not found in the CSV file.");
        }

        // Move the file to the processed/ prefix
        const processedKey = `processed/${objectKey}`;
        console.log(`Moving file to: ${processedKey}`);

        // Copy the object
        await s3.copyObject({
            Bucket: bucketName,
            CopySource: `${bucketName}/${objectKey}`,
            Key: processedKey,
        });
        console.log(`File copied to: ${processedKey}`);

        // Delete the original object
        await s3.deleteObject({ Bucket: bucketName, Key: objectKey });
        console.log(`Original file deleted: ${objectKey}`);

        // Start the Step Function workflow
        const input = JSON.stringify({
            bucketName,
            objectKey: processedKey, // Pass the new key to the Step Function
            filePreview: fileContent.substring(0, 100), // Example: send a preview of the file to Step Functions
        });

        const command = new StartExecutionCommand({
            stateMachineArn,
            input,
        });

        const response = await sfnClient.send(command);
        console.log("Step Function Execution Started:", response);

        return {
            statusCode: 200,
            body: JSON.stringify({
                message: "File processed successfully and stored in the processed/ folder.",
                executionArn: response.executionArn,
            }),
        };
    } catch (error) {
        console.error("Error processing file:", error);
        // return {
        //     statusCode: 500,
        //     body: JSON.stringify({
        //         message: "Error processing file",
        //         error: error.message,
        //     }),
        // };

        // Throw an error to indicate failure
        throw new Error(
            JSON.stringify({
                message: "Error processing file",
                error: error.message,
            })
        );
    }
    finally {
        if (db) {
            await db.close();
        }
    }
}
