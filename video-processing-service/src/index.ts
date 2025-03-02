import ffmpeg from "fluent-ffmpeg";
import { convertVideo, deleteProcessedVideo, deleteRawVideo, downloadRawVideo, setupDirectories, uploadProcessedVideo } from "./storage";
import express from "express";

setupDirectories();

const app = express()
app.use(express.json())
app.post("/process-video", async (req, res): Promise<any> => { 
    //will be invoked from cloud pub/sub
    let data;
    try {
        const message = Buffer.from(req.body.message.data, 'base64').toString('utf-8');
        data  = JSON.parse(message);
        if(!data.name) {
            throw new Error('Invalid message payload received.');
        }
    }
    catch (error) {
        console.error(error);
        return res.status(400).send('Bad Request: missing fileName');
    }
    
    const inputFileName = data.name;
    const outputFileName = `processed-${inputFileName}`;

    //download the raw from cloud
    await downloadRawVideo(inputFileName);

    try {
        await convertVideo(inputFileName, outputFileName);
    } catch (error) {
        await Promise.all([
            deleteProcessedVideo(outputFileName),
             deleteRawVideo(inputFileName)
            ]); 
        console.error(error);
        return res.status(500).send('Internal server errror: processing failed');
    }

    //Upload the processedVideo
    await uploadProcessedVideo(outputFileName)

    await Promise.all([ 
        deleteProcessedVideo(outputFileName),
        deleteRawVideo(inputFileName)
        ]); 
    
    return res.status(200).send('Processing finished successfully');
});

const port = process.env.PORT || 3000
app.listen(port, () => {
    console.log(`Video proc service listening at http://localhost: ${port}`);
});