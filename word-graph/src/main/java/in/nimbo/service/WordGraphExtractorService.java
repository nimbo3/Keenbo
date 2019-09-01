package in.nimbo.service;

import com.vdurmont.emoji.EmojiManager;
import in.nimbo.common.config.HBaseConfig;
import in.nimbo.common.entity.GraphResult;
import in.nimbo.common.utility.LinkUtility;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import org.graphframes.GraphFrame;

import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.lit;

public class WordGraphExtractorService {
    private WordGraphExtractorService() {
    }

    public static GraphResult extract(HBaseConfig hBaseConfig, SparkSession spark, JavaRDD<Result> hBaseRDD) {
        String badWordRegex = ".*\\-.*";
        String tagRegex = "^<.*?>$";
        String idRegex = "^@.*$";
        String linkRegex = "^http[s]?:.*$";
        String starRegex = "^\\*+$";
        String numberRegex = ".*[0-9].*";
        List<String> badWords = Arrays.asList("jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct",
                "nov", "dec", "use", "ago", "new", "jan.", "feb.", "mar.", "apr.", "may.", "jun.", "jul.", "aug.",
                "sep.", "oct.", "nov.", "dec.", "==", "!!", "☁️");

        byte[] rankColumn = hBaseConfig.getPageRankColumn();
        byte[] anchorColumnFamily = hBaseConfig.getAnchorColumnFamily();
        byte[] dataColumnFamily = hBaseConfig.getDataColumnFamily();

        JavaRDD<Cell> hBaseCellsRDD = hBaseRDD.flatMap(result -> result.listCells().iterator());
        hBaseCellsRDD.persist(StorageLevel.MEMORY_AND_DISK());

        JavaRDD<Row> pageContentRDD = hBaseCellsRDD
                .filter(cell -> CellUtil.matchingFamily(cell, dataColumnFamily)
                        && !CellUtil.matchingQualifier(cell, rankColumn))
                .map(cell -> RowFactory.create(
                        Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength()),
                        Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength())))
                .filter(row -> {
                    String word = row.getString(1);
                    return !word.matches(idRegex) && !word.matches(linkRegex) && !word.matches(starRegex)
                            && !word.matches(tagRegex) && !word.matches(numberRegex) && !word.matches(badWordRegex)
                            && !badWords.contains(word) && !EmojiManager.isEmoji(word);
                });

        JavaRDD<Row> edges = hBaseCellsRDD
                .filter(cell -> CellUtil.matchingFamily(cell, anchorColumnFamily))
                .map(cell -> {
                    String source = Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
                    String destination = Bytes.toString(cell.getQualifierArray()
                            , cell.getQualifierOffset(), cell.getQualifierLength());
                    int index = destination.indexOf('#');
                    if (index != -1)
                        destination = destination.substring(0, index);
                    return RowFactory.create(source, LinkUtility.reverseLink(destination));
                }).filter(domain -> !domain.getString(0).equals(domain.getString(1)));

        JavaRDD<Row> selfEdge = hBaseRDD
                .map(result -> {
                    String link = Bytes.toString(result.getRow());
                    return RowFactory.create(link, link);
                }).distinct();

        JavaRDD<Row> finalEdges = edges.union(selfEdge);

        StructType pageNodeSchema = new StructType(new StructField[]{
                new StructField("id", DataTypes.StringType, false, Metadata.empty()),
                new StructField("content", DataTypes.StringType, false, Metadata.empty()),
        });

        StructType edgesSchema = new StructType(new StructField[]{
                new StructField("src", DataTypes.StringType, false, Metadata.empty()),
                new StructField("dst", DataTypes.StringType, false, Metadata.empty()),
        });

        Dataset<Row> pageVertexDF = spark.createDataFrame(pageContentRDD, pageNodeSchema);
        Dataset<Row> edgeDF = spark.createDataFrame(finalEdges, edgesSchema);
        edgeDF.repartition(32);
        hBaseCellsRDD.unpersist();

        pageVertexDF = pageVertexDF.groupBy(col("id")).agg(collect_list("content").alias("keywords"));
        GraphFrame graphFrame = new GraphFrame(pageVertexDF, edgeDF);
        Dataset<Row> keywords = graphFrame
                .triplets()
                .groupBy("src", "dst")
                .agg(functions.count(functions.lit(1)).alias("weight"));


        keywords = keywords.withColumn("word1", explode(col("src.keywords")))
                .withColumn("word2", explode(col("dst.keywords")))
                .select("word1", "word2", "weight")
                .filter(col("word1").notEqual(col("word2")));
        keywords.repartition(32);

        keywords = keywords.select(least("word1", "word2").alias("from")
                , greatest("word1", "word2").alias("to"), col("weight"))
                .groupBy("from", "to")
                .agg(functions.sum("weight").alias("width"))
                .sort(desc("width")).limit(500000);

        Dataset<Row> verticesPart1 = keywords.select(col("from")).dropDuplicates();
        Dataset<Row> verticesPart2 = keywords.select(col("to")).dropDuplicates();
        Dataset<Row> vertices = verticesPart1.union(verticesPart2)
                .select(col("from").as("id")).dropDuplicates();
        vertices = vertices.withColumn("font", struct(lit(1.0)));

        StructType verticesSchema = new StructType(new StructField[]{
                new StructField("id", DataTypes.StringType, false, Metadata.empty()),
                new StructField("font", DataTypes.createStructType(
                        new StructField[]{
                                new StructField("size", DataTypes.DoubleType, false, Metadata.empty())
                        }
                ), false, Metadata.empty())});
        Dataset<Row> vertexDF = spark.createDataFrame(vertices.toJavaRDD(), verticesSchema);

        return new GraphResult(vertexDF, keywords);
    }
}
