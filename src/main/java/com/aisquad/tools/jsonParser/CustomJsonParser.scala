package com.aisquad.tools.jsonParser
/*
//@Author: Shashank Shukla
*/
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.explode

import java.sql.Timestamp

import java.io.PrintWriter
import java.io.FileWriter
import java.io.File
import java.io.FileNotFoundException
import java.io.BufferedReader
import java.io.FileReader
import java.io.BufferedWriter
import java.io.FileOutputStream
import java.io.OutputStreamWriter
import java.io.InputStreamReader
import java.io.FileInputStream

//Standard scala object
object CustomJsonParser {
  //scala main function, accepts command line arguments.
  def main(args: Array[String]): Unit = {

    //Setting Hadoop home
    //the .exe file in the folder as per below.
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    //to get execution timestamp.
    val timestamp = new Timestamp(System.currentTimeMillis())
    val timeForpath = timestamp.getTime.toString()

    // Spark Session creation
    val spark = SparkSession.builder().master("local[*]").appName("CustomJsonParser").getOrCreate()
    import spark.implicits._

    // Spark Context defined
    val sc = spark.sparkContext
    val sqlcontext = new SQLContext(sc)
    
    //case to handle multiple json files.
    if (args(0).contains("*.json")) {
      var folder1 = new File(args(0).replaceFirst("\\*.json", ""))
      var listOfFiles = folder1.list()

      new File(args(0).replaceFirst("\\*.json", "") + "newJson").mkdirs();
      var I = ""
      //loop for picking files one by one and convert them to inline nested json.
      for (I <- listOfFiles) {
        if (!I.equalsIgnoreCase("newJson")) {
          var outputStreamWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(args(0).replaceFirst("\\*.json", "") + "newJson\\" + I), "UTF-8"));
          var br1 = new BufferedReader(new InputStreamReader(new FileInputStream(args(0).replaceFirst("\\*.json", "") + I), "UTF8"))
          var inputFileLine = br1.readLine()

          while (inputFileLine != null) {
            outputStreamWriter.write(inputFileLine.replace("\t", ""))
            outputStreamWriter.flush()
            inputFileLine = br1.readLine()
          }

          br1.close()
          outputStreamWriter.close()
        }
      }
      args(0) = args(0).replaceFirst("\\*.json", "") + "newJson\\"

    } 
    //case to handle single json file.
    else {
      var outputStreamWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(args(0).replaceFirst("\\.json", "_new\\.json")), "UTF-8"));
      var br1 = new BufferedReader(new InputStreamReader(new FileInputStream(args(0)), "UTF8"))
      var inputFileLine = br1.readLine()

      while (inputFileLine != null) {
        outputStreamWriter.write(inputFileLine.replace("\t", ""))
        outputStreamWriter.flush()
        inputFileLine = br1.readLine()
      }
      br1.close()
      outputStreamWriter.close()
      args(0) = args(0).replaceFirst("\\.json", "_new\\.json")
    }

    //Option 1
    val df = sqlcontext.read.json(args(0))

    //Option 2
    val df2 = sqlcontext.read.json(args(0)).createOrReplaceTempView("target_table")

    //Exporting Json Schema
    new PrintWriter(args(2) + "\\JsonSchema.txt") { write(df.schema.treeString); close }

    //Accept custom sql query to extract the required columns.
    val finalResult = sqlcontext.sql(args(1)).toDF()

    //writing the output in csv file
    finalResult.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save(args(2) + "\\Result" + "\\" + timeForpath + "\\queryOutput")

    //Option in which spark dataframes are getting created.
    val risks = df.select(explode($"data.risks").as("risks")).select($"risks.caseReferenceNumber", $"risks.category", $"risks.id", $"risks.lastUpdateTimeStamp", $"risks.lastUpdateUser", $"risks.result", $"risks.ruleId", $"risks.score")
    risks.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save(args(2) + "\\Result" + "\\" + timeForpath + "\\risks")

    //flattening sample for other structs
    val reviewCase = df.select($"data.reviewCase.accountOpenDate", $"data.reviewCase.appendReviewEvents", $"data.reviewCase.archivalDateTime", $"data.reviewCase.bin", $"data.reviewCase.caseCompletionDate", $"data.reviewCase.caseDecision", $"data.reviewCase.caseStatus", $"data.reviewCase.caseSubmitDate", $"data.reviewCase.currencyAccount", $"data.reviewCase.division", $"data.reviewCase.existingPep", $"data.reviewCase.existingRiskRate", $"data.reviewCase.isSwitchRequired", $"data.reviewCase.keyStage", $"data.reviewCase.lastUpdateTimeStamp", $"data.reviewCase.lastUpdateUser", $"data.reviewCase.nextAnnualReviewDT", $"data.reviewCase.populationGroup", $"data.reviewCase.portfolioCode", $"data.reviewCase.preferredOnlineAccess", $"data.reviewCase.reasonForDecision", $"data.reviewCase.resolveOnshoreExcp", $"data.reviewCase.retentionCode", explode($"data.reviewCase.reviewEvents").as("reviewEvents"), $"data.reviewCase.reviewOriginSystem", $"data.reviewCase.rmName", $"data.reviewCase.sourceRefNum", $"data.reviewCase.staffStartDate", $"data.reviewCase.staffSubmitDate", $"data.reviewCase.transactionReviewComplete").select($"accountOpenDate", $"appendReviewEvents", $"archivalDateTime", $"bin", $"caseCompletionDate", $"caseDecision", $"caseStatus", $"caseSubmitDate", $"currencyAccount", $"division", $"existingPep", $"existingRiskRate", $"isSwitchRequired", $"keyStage", $"lastUpdateTimeStamp", $"lastUpdateUser".as("reviewCase_lastUpdateUser"), $"nextAnnualReviewDT", $"populationGroup", $"portfolioCode", $"preferredOnlineAccess", $"reasonForDecision", $"resolveOnshoreExcp", $"reviewEvents.addressChangeType", $"reviewEvents.alertCategory", $"reviewEvents.alertDate", $"reviewEvents.alertSourceSystem", $"reviewEvents.caseReferenceNumber", $"reviewEvents.createDate", $"reviewEvents.eventDescription", $"reviewEvents.eventID", $"reviewEvents.eventSubType", $"reviewEvents.eventType", $"reviewEvents.freeForm", $"reviewEvents.initiatorName", $"reviewEvents.lastUpdateTime", explode($"reviewEvents.details").as("details"), $"lastUpdateUser".as("reviewEvents_lastUpdateUser"), $"reviewOriginSystem", $"rmName", $"sourceRefNum", $"staffStartDate", $"staffSubmitDate", $"transactionReviewComplete").select($"accountOpenDate", $"appendReviewEvents", $"archivalDateTime", $"bin", $"caseCompletionDate", $"caseDecision", $"caseStatus", $"caseSubmitDate", $"currencyAccount", $"division", $"existingPep", $"existingRiskRate", $"isSwitchRequired", $"keyStage", $"lastUpdateTimeStamp", $"reviewCase_lastUpdateUser", $"nextAnnualReviewDT", $"populationGroup", $"portfolioCode", $"preferredOnlineAccess", $"reasonForDecision", $"resolveOnshoreExcp", $"addressChangeType", $"alertCategory", $"alertDate", $"alertSourceSystem", $"caseReferenceNumber", $"createDate", $"details.caseReferenceNumber", $"details.id", $"details.lastUpdateTimeStamp", $"details.lastUpdateUser", $"details.newData", $"details.oldData", $"details.party", $"details.partyId", $"eventDescription", $"eventID", $"eventSubType", $"eventType", $"freeForm", $"initiatorName", $"lastUpdateTime", $"reviewEvents_lastUpdateUser", $"reviewOriginSystem", $"rmName", $"sourceRefNum", $"staffStartDate", $"staffSubmitDate", $"transactionReviewComplete")

    val kyc_individuals = df.select(explode($"data.kyc.individuals").as("individuals")).select($"individuals.bin", $"individuals.birthDate", $"individuals.caseReferenceNumber".as("individuals_caseReferenceNumber"), $"individuals.contactMobile", $"individuals.countryCode", $"individuals.countryOfBirth", $"individuals.ekycFlag", $"individuals.email", explode($"individuals.exceptions").as("exceptions"), $"individuals.financialOwnership", $"individuals.firstName", $"individuals.franchise", $"individuals.fromBureau", $"individuals.fullName", $"individuals.gender", $"individuals.id".as("individuals_id"), $"individuals.isPointOfContact", $"individuals.lastUpdateTimeStamp".as("individuals_lastUpdateTimeStamp"), $"individuals.lastUpdateUser".as("individuals_lastUpdateUser"), $"individuals.memorableWord", $"individuals.middleName", $"individuals.nationality", $"individuals.notLinked", $"individuals.partyName", $"individuals.pepMMerApplied", $"individuals.persona", $"individuals.personalAccount", $"individuals.portfolioCode", $"individuals.positions", $"individuals.presentPreviousAddresses", $"individuals.registeredTaxOutsideUk", $"individuals.signMoreThan50K", $"individuals.surname", $"individuals.title", $"individuals.vMMerApplied", $"individuals.votingOwnership").select($"bin", $"birthDate", $"individuals_caseReferenceNumber", $"contactMobile", $"countryCode", $"countryOfBirth", $"ekycFlag", $"email", $"exceptions.caseReferenceNumber", $"exceptions.code", $"exceptions.details", $"exceptions.id".as("exceptions_id"), $"exceptions.lastUpdateTimeStamp".as("exceptions_lastUpdateTimeStamp"), $"exceptions.lastUpdateUser".as("exceptions_lastUpdateUser"), $"financialOwnership", $"firstName", $"franchise", $"fromBureau", $"fullName", $"gender", $"individuals_id", $"isPointOfContact", $"individuals_lastUpdateTimeStamp", $"individuals_lastUpdateUser", $"memorableWord", $"middleName", $"nationality", $"notLinked", $"partyName", $"pepMMerApplied", $"persona", $"personalAccount", $"portfolioCode", $"positions", explode($"presentPreviousAddresses").as("presentPreviousAddresses"), $"registeredTaxOutsideUk", $"signMoreThan50K", $"surname", $"title", $"vMMerApplied", $"votingOwnership").select($"bin", $"birthDate", $"individuals_caseReferenceNumber", $"contactMobile", $"countryCode", $"countryOfBirth", $"ekycFlag", $"email", $"caseReferenceNumber", $"code", $"details", $"exceptions_id", $"exceptions_lastUpdateTimeStamp", $"exceptions_lastUpdateUser", $"financialOwnership", $"firstName", $"franchise", $"fromBureau", $"fullName", $"gender", $"individuals_id", $"isPointOfContact", $"individuals_lastUpdateTimeStamp", $"individuals_lastUpdateUser", $"memorableWord", $"middleName", $"nationality", $"notLinked", $"partyName", $"pepMMerApplied", $"persona", $"personalAccount", $"portfolioCode", $"positions", $"presentPreviousAddresses.addrMovedDate", $"presentPreviousAddresses.country", $"presentPreviousAddresses.id", $"presentPreviousAddresses.lastUpdateTimeStamp", $"presentPreviousAddresses.lastUpdateUser", $"presentPreviousAddresses.line1", $"presentPreviousAddresses.line2", $"presentPreviousAddresses.line3", $"presentPreviousAddresses.line4", $"presentPreviousAddresses.postCode", $"registeredTaxOutsideUk", $"signMoreThan50K", $"surname", $"title", $"vMMerApplied", $"votingOwnership")

    val kyc_companies = df.select(explode($"data.kyc.companies").as("companies")).select($"companies.bin", $"companies.caseReferenceNumber", $"companies.countryOfIncorporation", $"companies.ekycFlag", explode($"companies.exceptions").as("exceptions"), $"companies.financialOwnership", $"companies.franchise", $"companies.fromBureau", $"companies.id".as("companies_id"), $"companies.lastUpdateTimeStamp".as("companies_lastUpdateTimeStamp"), $"companies.lastUpdateUser".as("companies_lastUpdateUser"), $"companies.notLinked", $"companies.partyName", $"companies.persona", $"companies.portfolioCode", $"companies.positions", $"companies.presentPreviousAddresses", $"companies.taxOutsideUk", $"companies.tradeStartDate", $"companies.tradingAddress.country", $"companies.tradingAddress.id".as("tradingAddress_id"), $"companies.tradingAddress.lastUpdateTimeStamp".as("tradingAddress_lastUpdateTimeStamp"), $"companies.tradingAddress.lastUpdateUser".as("tradingAddress_lastUpdateUser"), $"companies.tradingAddress.line1", $"companies.tradingAddress.line2", $"companies.tradingAddress.line3", $"companies.tradingAddress.line4", $"companies.tradingAddress.postCode").select($"bin", $"caseReferenceNumber", $"countryOfIncorporation", $"ekycFlag", $"exceptions.caseReferenceNumber", $"exceptions.code", $"exceptions.details", $"exceptions.id", $"exceptions.lastUpdateTimeStamp", $"exceptions.lastUpdateUser", $"financialOwnership", $"franchise", $"fromBureau", $"companies_id", $"companies_lastUpdateTimeStamp", $"companies_lastUpdateUser", $"notLinked", $"partyName", $"persona", $"portfolioCode", $"positions", $"presentPreviousAddresses", $"taxOutsideUk", $"tradeStartDate", $"country", $"tradingAddress_id", $"tradingAddress_lastUpdateTimeStamp", $"tradingAddress_lastUpdateUser", $"line1", $"line2", $"line3", $"line4", $"postCode")

    val kybs = df.select(explode($"data.kybs").as("kybs")).select($"kybs.annualTurnOver", $"kybs.bin", $"kybs.bureauRegistrationAddress.country".as("bureauRegistrationAddress_country"), $"kybs.bureauRegistrationAddress.id".as("bureauRegistrationAddress_id"), $"kybs.bureauRegistrationAddress.lastUpdateTimeStamp".as("bureauRegistrationAddress_lastUpdateTimeStamp"), $"kybs.bureauRegistrationAddress.lastUpdateUser".as("bureauRegistrationAddress_lastUpdateUser"), $"kybs.bureauRegistrationAddress.line1".as("bureauRegistrationAddress_line1"), $"kybs.bureauRegistrationAddress.line2".as("bureauRegistrationAddress_line2"), $"kybs.bureauRegistrationAddress.postCode".as("bureauRegistrationAddress_postCode"), $"kybs.caseReferenceNumber", $"kybs.correspondenceAddress.country".as("correspondenceAddress_country"), $"kybs.correspondenceAddress.id".as("correspondenceAddress_id"), $"kybs.correspondenceAddress.lastUpdateTimeStamp".as("correspondenceAddress_lastUpdateTimeStamp"), $"kybs.correspondenceAddress.lastUpdateUser".as("correspondenceAddress_lastUpdateUser"), $"kybs.correspondenceAddress.line1".as("correspondenceAddress_line1"), $"kybs.correspondenceAddress.line2".as("correspondenceAddress_line2"), $"kybs.correspondenceAddress.postCode".as("correspondenceAddress_postCode"), $"kybs.correspondenceAddressChoice", $"kybs.correspondenceAddressSame", $"kybs.currencyAccountFlag", $"kybs.declarations", explode($"kybs.financialEEEW").as("financialEEEW"), $"kybs.financialMovement", $"kybs.franchise", $"kybs.freeForm", $"kybs.genericDetails", $"kybs.haveOtherTradeName", $"kybs.id", $"kybs.injectionsOfFunds", $"kybs.internationalPayment", $"kybs.internationalTrades", $"kybs.isCharity", $"kybs.lastUpdateTimeStamp", $"kybs.lastUpdateUser", $"kybs.numberOfEmployees", $"kybs.partyName", $"kybs.partySubType", $"kybs.pastAccountTurnover", $"kybs.persona", $"kybs.portfolioCode", $"kybs.products", $"kybs.registeredForTaxNonUk", $"kybs.registeredInUk", $"kybs.registrationAddress.country".as("registrationAddress_country"), $"kybs.registrationAddress.id".as("registrationAddress_id"), $"kybs.registrationAddress.lastUpdateTimeStamp".as("registrationAddress_lastUpdateTimeStamp"), $"kybs.registrationAddress.lastUpdateUser".as("registrationAddress_lastUpdateUser"), $"kybs.registrationAddress.line1".as("registrationAddress_line1"), $"kybs.registrationAddress.line2".as("registrationAddress_line2"), $"kybs.registrationAddress.postCode".as("registrationAddress_postCode"), $"kybs.reviewCrn", $"kybs.taxDetails", $"kybs.tradeStartDate", $"kybs.tradingActivities", $"kybs.tradingAddress.country".as("tradingAddress_country"), $"kybs.tradingAddress.id".as("tradingAddress_id"), $"kybs.tradingAddress.lastUpdateTimeStamp".as("tradingAddress_lastUpdateTimeStamp"), $"kybs.tradingAddress.lastUpdateUser".as("tradingAddress_lastUpdateUser"), $"kybs.tradingAddress.line1".as("tradingAddress_line1"), $"kybs.tradingAddress.line2".as("tradingAddress_line2"), $"kybs.tradingAddress.postCode".as("tradingAddress_postCode"), $"kybs.tradingAddressSameAsRegional", $"kybs.trustClassOfBeneficiaries").select($"annualTurnOver", $"bin", $"bureauRegistrationAddress_country", $"bureauRegistrationAddress_id", $"bureauRegistrationAddress_lastUpdateTimeStamp", $"bureauRegistrationAddress_lastUpdateUser", $"bureauRegistrationAddress_line1", $"bureauRegistrationAddress_line2", $"bureauRegistrationAddress_postCode", $"caseReferenceNumber", $"correspondenceAddress_country", $"correspondenceAddress_id", $"correspondenceAddress_lastUpdateTimeStamp", $"correspondenceAddress_lastUpdateUser", $"correspondenceAddress_line1", $"correspondenceAddress_line2", $"correspondenceAddress_postCode", $"correspondenceAddressChoice", $"correspondenceAddressSame", $"currencyAccountFlag", $"declarations", $"financialEEEW.caseReferenceNumber", $"financialEEEW.id".as("financialEEEW_id"), $"financialEEEW.lastUpdateTimeStamp".as("financialEEEW_lastUpdateTimeStamp"), $"financialEEEW.lastUpdateUser".as("financialEEEW_lastUpdateUser"), $"financialEEEW.paymentType", $"financialMovement", $"franchise", $"freeForm", $"genericDetails", $"haveOtherTradeName", $"id", $"injectionsOfFunds", $"internationalPayment", $"internationalTrades", $"isCharity", $"lastUpdateTimeStamp", $"lastUpdateUser", $"numberOfEmployees", $"partyName", $"partySubType", $"pastAccountTurnover", $"persona", $"portfolioCode", $"products", $"registeredForTaxNonUk", $"registeredInUk", $"registrationAddress_country", $"registrationAddress_id", $"registrationAddress_lastUpdateTimeStamp", $"registrationAddress_lastUpdateUser", $"registrationAddress_line1", $"registrationAddress_line2", $"registrationAddress_postCode", $"reviewCrn", $"taxDetails", $"tradeStartDate", $"tradingActivities", $"tradingAddress_country", $"tradingAddress_id", $"tradingAddress_lastUpdateTimeStamp", $"tradingAddress_lastUpdateUser", $"tradingAddress_line1", $"tradingAddress_line2", $"tradingAddress_postCode", $"tradingAddressSameAsRegional", $"trustClassOfBeneficiaries")

    //Printing the results of flat dataframe to console.
    finalResult.show()
    risks.show()
    reviewCase.show()
    kyc_individuals.show()
    kyc_companies.show()
    kybs.show()
  }
}

