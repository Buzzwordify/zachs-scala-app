package spring.boot.scala.example.controller

import java.time.LocalDateTime
import scala.collection.JavaConverters._
import io.micrometer.core.annotation.Timed
import scala.collection.mutable.HashMap;
import scala.collection.mutable.Map;
import org.springframework.beans.factory.annotation.Value
import org.springframework.web.bind.annotation.RequestMethod.GET
import org.springframework.web.bind.annotation.{RequestMapping, RequestParam, RestController}
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;

@RestController
class RootController {
  @Value("${application.name}")
  val appName: String = null

  @RequestMapping(path = Array("/"), method = Array(GET))
  @Timed
  def root(): Map[String, Any] = {
    Map("name" -> appName,
      "java.version" -> System.getProperty("java.version"),
      "time_of_access" -> LocalDateTime.now())
  }

  @RequestMapping(path = Array("/insert"))
  @Timed
  def insert(@RequestParam("title") title: String, @RequestParam("year") year: Int, @RequestParam("review") review: String, @RequestParam("rating") rating: Int): Unit = {
    val client = AmazonDynamoDBClientBuilder.standard()
            .withRegion(Regions.US_WEST_2)
            .withCredentials(new ProfileCredentialsProvider("<FMI>", "<FMI>"))
            .build();
    val dynamoDB = new DynamoDB(client);
    val table = dynamoDB.getTable("Movies2");
    var ratingAndReview = Map("review"-> review, "rating"-> rating).asJava
    //try {
      println("Adding a new item...");
      val outcome = table
            .putItem(new Item().withPrimaryKey("year", year, "title", title).withMap("info", ratingAndReview))
            //.withReturnValues(ReturnValue.ALL_OLD);

      println("PutItem succeeded:\n" + outcome.getPutItemResult());

    /*}
    catch {
          case _ : Throwable => println("Got some other kind of Throwable exception")
    }*/
  }

  @RequestMapping(path = Array("/dynamodb"), method = Array(GET))
  @Timed
  def dynamodb(@RequestParam(defaultValue = "Shrek") title: String, @RequestParam(defaultValue = "2001") year: Int): Option[Item] = {
    val client = AmazonDynamoDBClientBuilder.standard()
          .withRegion(Regions.US_WEST_2)
          .withCredentials(new ProfileCredentialsProvider("<FMI>", "<FMI>"))
          .build();
    val dynamoDB = new DynamoDB(client);
    val table = dynamoDB.getTable("Movies2");

    try{
        val spec = new GetItemSpec().withPrimaryKey("year", year, "title", title);
        println(spec.toString());
        println("Attempting to read the item");
        val outcome = table.getItem(spec);
        println("GetItem succeeded: " + outcome);
        Some(outcome)
    }
    catch{
        case aws: ResourceNotFoundException => {
          println(s"Movie $title from $year not found in dynamoDB table Movies2")
          None
        }
        case _ : Throwable =>{
          println("Got some other kind of Throwable exception")
          None
        }
    }
  }

  @RequestMapping(path = Array("/shrek"), method = Array(GET))
  @Timed
  def shrek(): Map[String, Any] = {
    Map("movie" -> "Shrek",
      "year" -> "2001", 
      "rating" -> 10,
      "review" -> "What're u doin in my swamp",
      "time_of_access" -> LocalDateTime.now())
  }

}
