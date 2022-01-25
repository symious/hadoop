package org.apache.hadoop.tools.mapred;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileRecordReader;
import org.apache.hadoop.tools.CopyListingFileStatus;
import org.apache.hadoop.tools.DistCpConstants;
import org.apache.hadoop.tools.util.DistCpUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class UniformBlockInputFormat
        extends InputFormat<Text, CopyListingFileStatus> {
    private static final Log LOG
            = LogFactory.getLog(UniformBlockInputFormat.class);

    @Override
    public List<InputSplit> getSplits(JobContext context)
            throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();
        int numSplits = DistCpUtils.getInt(configuration,
                JobContext.NUM_MAPS);

        if (numSplits == 0) return new ArrayList<InputSplit>();

        return getSplits(configuration, numSplits,
                DistCpUtils.getLong(configuration,
                        DistCpConstants.CONF_LABEL_TOTAL_BLOCKS_TO_BE_COPIED));
    }

    private List<InputSplit> getSplits(Configuration configuration, int numSplits,
                                       long totalBlocksCount) throws IOException {
        List<InputSplit> splits = new ArrayList<InputSplit>(numSplits);
        long nBlocksPerSplit = (long) Math.ceil(totalBlocksCount * 1.0 / numSplits);

        CopyListingFileStatus srcFileStatus = new CopyListingFileStatus();
        Text srcRelPath = new Text();
        long currentSplitBlocks = 0;
        long currentSplitSize = 0;
        long lastSplitStart = 0;
        long lastPosition = 0;

        final Path listingFilePath = getListingFilePath(configuration);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Average blocks per map: " + nBlocksPerSplit +
                    ", Number of maps: " + numSplits + ", total blocks: " + totalBlocksCount);
        }
        SequenceFile.Reader reader=null;
        try {
            reader = getListingFileReader(configuration);
            while (reader.next(srcRelPath, srcFileStatus)) {
                // If adding the current file would cause the blocks per map to exceed
                // limit. Add the current file to new split
                if (currentSplitBlocks + srcFileStatus.getBlocksCount() > nBlocksPerSplit
                        && lastPosition != 0) {
                    FileSplit split = new FileSplit(listingFilePath, lastSplitStart,
                            lastPosition - lastSplitStart, null);
                    if (LOG.isDebugEnabled()) {
                        LOG.debug ("Creating split : " + split + ", bytes in split: " + currentSplitSize
                                + ", blocks in split: " + currentSplitBlocks);
                    }
                    splits.add(split);
                    lastSplitStart = lastPosition;
                    currentSplitSize = 0;
                    currentSplitBlocks = 0;
                }
                currentSplitSize += srcFileStatus.getChunkLength();
                currentSplitBlocks += srcFileStatus.getBlocksCount();
                lastPosition = reader.getPosition();
            }
            if (lastPosition > lastSplitStart) {
                FileSplit split = new FileSplit(listingFilePath, lastSplitStart,
                        lastPosition - lastSplitStart, null);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Creating split : " + split + ", bytes in split: "
                            + currentSplitSize + ", blocks in split: " + currentSplitBlocks);
                }
                splits.add(split);
            }

        } finally {
            IOUtils.closeStream(reader);
        }

        return splits;
    }

    private static Path getListingFilePath(Configuration configuration) {
        final String listingFilePathString =
                configuration.get(DistCpConstants.CONF_LABEL_LISTING_FILE_PATH, "");

        assert !listingFilePathString.equals("")
                : "Couldn't find listing file. Invalid input.";
        return new Path(listingFilePathString);
    }

    private SequenceFile.Reader getListingFileReader(Configuration configuration) {

        final Path listingFilePath = getListingFilePath(configuration);
        try {
            final FileSystem fileSystem = listingFilePath.getFileSystem(configuration);
            if (!fileSystem.exists(listingFilePath))
                throw new IllegalArgumentException("Listing file doesn't exist at: "
                        + listingFilePath);

            return new SequenceFile.Reader(configuration,
                    SequenceFile.Reader.file(listingFilePath));
        }
        catch (IOException exception) {
            LOG.error("Couldn't find listing file at: " + listingFilePath, exception);
            throw new IllegalArgumentException("Couldn't find listing-file at: "
                    + listingFilePath, exception);
        }
    }

    /**
     * Implementation of InputFormat::createRecordReader().
     * @param split The split for which the RecordReader is sought.
     * @param context The context of the current task-attempt.
     * @return A SequenceFileRecordReader instance, (since the copy-listing is a
     * simple sequence-file.)
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public RecordReader<Text, CopyListingFileStatus> createRecordReader(
            InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
        return new SequenceFileRecordReader<Text, CopyListingFileStatus>();
    }
}
