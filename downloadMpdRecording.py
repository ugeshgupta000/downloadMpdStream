import shutil

import youtube_dl
import requests
import os
import concurrent.futures

'''
Resources and commands:

## https://dashod.akamaized.net/media/cv/events/27/38/81/1/rt/1_fhvideo1_1610651642037_mpd/audio/en/init.mp4
## https://dashod.akamaized.net/media/cv/events/27/38/81/1/rt/1_fhvideo1_1610651642037_mpd/video/2/init.mp4
## https://dashod.akamaized.net/media/cv/events/27/38/81/1/rt/1_fhvideo1_1610651642037_mpd/video/2/seg-22.m4f

We keep downloading segments until they error out, which means no segment is left.

# Find all formats available for a mpd file
youtube-dl -civw -F https://dashod.akamaized.net/media/cv/events/27/38/81/1/rt/1_fhvideo1_1610651642037_mpd/stream.mpd 

# Download specific format directly.
youtube-dl -civw --format audio_en https://dashod.akamaized.net/media/cv/events/27/38/81/5/rt/1_fhvideo1_1610737801553_mpd/stream.mpd

# Download using external download aria2c
youtube-dl https://dashod.akamaized.net/media/cv/events/27/38/81/5/rt/1_fhvideo1_1610737801553_mpd/stream.mpd -f audio_en -civw --external-downloader aria2c --external-downloader-args "-c -j 16 -s 16 -x 16 -k 5M"

Join Audio video:
ffmpeg -f concat -safe 0 -i audio.manifest -c copy audio.m4a

'''

# Extract the folder name from mpd File
def findRequestedFormats(mpdUrl):
    ydl = youtube_dl.YoutubeDL({})

    result = ydl.extract_info(mpdUrl, download=False)
    return result['requested_formats']

# https://dashod.akamaized.net/media/cv/events/27/38/81/1/rt/1_fhvideo1_1610651642037_mpd/stream.mpd
# Return 1_fhvideo1_1610651642037_mpd
def getFolderName(mpdUrl):
    if mpdUrl.endswith('/'):
        mpdUrl = mpdUrl[:-1]
    return mpdUrl.rsplit('/', 2)[-2]

# Download each fragment, being called as thread handler
def downloadFragment(baseUrl, fragmentPath, baseFolder):
    outputFilePath = baseFolder + '/' + fragmentPath
    os.makedirs(os.path.dirname(outputFilePath), exist_ok=True)
    if not os.path.exists(outputFilePath):
        r = requests.get(baseUrl + fragmentPath)
        if r.status_code == 404:
            print('Unable to find fragment', outputFilePath)
            return
        with open(outputFilePath, 'wb') as f:
                f.write(r.content)

# This method downloads fragments in thread for each supported format
# and later creates one consolidated file for each format
def downloadFragments(baseFolder, requestedFormats):
    for requestedFormat in requestedFormats:
        baseUrl = requestedFormat['fragment_base_url']

        print('Downloading Fragments for', requestedFormat['format_id'])
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = []
            for fragment in requestedFormat['fragments']:
                futures.append(executor.submit(downloadFragment, baseUrl, fragment['path'], baseFolder))
            # Wait for all futures to complete
            concurrent.futures.wait(futures)

        # Now all fragments have been downloaded.
        # Lets combine them into a single file.
        # Below code is very fast even for 1GB file as well(That's why not checking for existence)
        # Cat is taking huge time and also causes errors, so lets refrain from that
        outputFilePath = baseFolder + '/' + requestedFormat['format_id'] + '.' + requestedFormat['ext']
        print('Creating combined file', outputFilePath)
        inputFiles = [baseFolder + '/' + fragment['path'] for fragment in requestedFormat['fragments']]
        with open(outputFilePath, "wb") as outFile:
            for inputFile in inputFiles:
                with open(inputFile, "rb") as inFile:
                    outFile.write(inFile.read())

# This method just joins the output for each format into a single mkv file.
def joinFragmentOutputs(baseFolder, requestedFormats, recordingName):
    inputs = ' '.join(['-i ' + baseFolder + '/' + reqFormat['format_id'] + '.' + reqFormat['ext'] for reqFormat in requestedFormats])
    os.system("ffmpeg " + inputs + " -c copy \"" + recordingName + ".mkv\"")


def main():
    mpdUrl = input('Enter a mpd url(We can find one by going to recording and in network request, selecting media only: ')
    recordingName = input('Enter recording name:')
    baseFolder = getFolderName(mpdUrl)
    requestedFormats = findRequestedFormats(mpdUrl)
    downloadFragments(baseFolder, requestedFormats)
    joinFragmentOutputs(baseFolder, requestedFormats, recordingName)
    choice = input('Do you want to clear intermediate files(y/n):')
    if choice == 'y':
        shutil.rmtree(baseFolder)


if __name__ == '__main__':
    main()