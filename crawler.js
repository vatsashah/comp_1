var XLSX = require('xlsx')
var workbook = XLSX.readFile('ads_txt_crawler.xlsx');
var sheet_name_list = workbook.SheetNames;
var xlData = XLSX.utils.sheet_to_json(workbook.Sheets[sheet_name_list[1]]);
var demandLines = XLSX.utils.sheet_to_json(workbook.Sheets[sheet_name_list[6]]);
const Promise = require('bluebird');
var axios = require('axios')
var postmark = require("postmark");
var client = new postmark.ServerClient("4af3efb0-7598-4ef3-a493-1685c8a418ea");
// var fromMail = "vatsa.shah@lgads.tv";
// var ccMail = "jonathan.tran@lgads.tv,kailey.landow@lgads.tv,brennan.cross@lgads.tv,vatsa.shah@lgads.tv";

async function main() {
  var dump = xlData;
  await Promise.map(xlData, async (row,i) => {
    // var url = 'https://www.' + row.domain + '/app-ads.txt';
    var url = row.domain
    var seller_id = (row.seller_id).toString();
    var seller_id_line = "lgads.tv, " + seller_id + ", DIRECT";
    await axios.get(url)
    .then((response) => {
      dump[i].HTTPstatus = response.status;
      dump[i].mailStatus = "incorrect app-ads.txt domain mail sent";
      dump[i].countLinesPresent = 0;
      dump[i]["Seller id demand line"] = "app-ads.txt not found";
      var siteData = response.data;
      if(response.data === undefined){
        for (var j=0; j<demandLines.length; j++){
            dump[i][demandLines[j]["Demand Lines"]] = "app-ads.txt not found";
          }
        if (row["Require manual checkup"] == "No") {
          //   client.sendEmail({
          //     "From": fromMail,
          //     "To": row["Mail ID"],
          //     "Cc": ccMail,
          //     "Subject": "app-ads.txt not found",
          //     "HtmlBody": "<html><body style='font-size: 10pt; font-family: Arial, sans-serif;'><p>Hi "+ row["First Name"] + ",<br/><br/>" + "I hope you are doing well.<br/><br/>Can you please share with us your app-ads.txt url?<br/>We are not seeing any response to this url - " + url + "<br/><br/>" + 
          //     '<table style="WIDTH: 200px; border-bottom: 1px solid" cellSpacing="0" cellPadding="0" width="400" border="0"><tbody><tr><td style="FONT-SIZE: 12px; FONT-FAMILY: Arial, sans-serif; WIDTH: 200px; COLOR: #515151; line-height:18px; border-bottom: 1px solid; padding-bottom: 5px;" width="200" colspan="2"><span style="FONT-SIZE: 17px; FONT-FAMILY: Arial; margin-bottom: 8px;"><strong>Vatsa Shah</strong></span><br><span style="FONT-SIZE: 12px; FONT-FAMILY: Arial">Operations Team</span><br><strong>LG Ad Solutions</strong><br><br><span style="COLOR: #A50034;"><strong>M.</strong> <span style="COLOR: #606060">+91 8734900979</span><br></span><span style="COLOR: #A50034; "><strong>E. </strong><span style="COLOR: #606060">vatsa@lgads.tv</span></span><br><br><a href="http://lgads.tv" target="_blank" rel="noopener"><img style="" src="https://lgads.tv/wp-content/uploads/2021/10/LGAdsolutions-logo3.png" width="200"></a></td></tr><tr> <td style="WIDTH: 200px; PADDING-BOTTOM: 15px; PADDING-TOP: 15px" vAlign="middle" align="left"><span><a href="https://www.facebook.com/LG-Ads-104021011763496" target="_blank" rel="noopener"><img border="0" width="25" src="https://lgads.tv/wp-content/uploads/2021/08/icon_Fb.png" alt="facebook icon" style="border:0; height:24px; width:24px"></a>&nbsp;</span><span><a href="https://twitter.com/LG_Ads_Platform" target="_blank" rel="noopener"><img border="0" width="25" src="https://lgads.tv/wp-content/uploads/2021/08/icon_Twitter.png" alt="twitter icon" style="border:0; height:24px; width:24px"></a>&nbsp;</span><span><a href="https://www.youtube.com/channel/UCGk2PePBwslIKg8JfQrCG3Q" target="_blank" rel="noopener"><img border="0" width="25" src="https://lgads.tv/wp-content/uploads/2021/08/icon_youtube.png" alt="youtube icon" style="border:0; height:24px; width:24px"></a>&nbsp;</span><span><a href="https://www.linkedin.com/company/lgads" target="_blank" rel="noopener"><img border="0" width="25" src="https://lgads.tv/wp-content/uploads/2021/08/icon_Linkedin.png" alt="linkedin icon" style="border:0; height:24px; width:24px"></a>&nbsp;</span></td></tr></tbody></table></body>'
          // });
        }
      }
      else {
        var missingLines = "";
        var srNos = 0;
        if (siteData.includes(seller_id)){ 
          dump[i]["Seller id demand line"] = "Present";
        }
        else {
          srNos += 1;
          missingLines += srNos.toString() + ".  <b>" + seller_id_line + "</b>  <i>(Important to add this line as it will help our DSPs to verify that we are a direct buyer of your inventory)</i><br/>";
          dump[i]["Seller id demand line"] = "Not Present";
        }
        for (var j=0; j<demandLines.length; j++){
            var s1 = demandLines[j]["s1"];
            var s2 = demandLines[j]["s2"];
            var s4 = demandLines[j]["s4"];
            if(s4 == undefined) s4 = "";
            var reseller = demandLines[j]["Demand Lines"].replace("DIRECT","RESELLER");
            var region = demandLines[j]["Country"]
            // var direct = demandLines[j]["Demand Lines"].replace("RESELLER", "DIRECT");
            if (siteData.includes(s1) & siteData.includes(s2) & siteData.includes(s4)){
              dump[i][reseller] = "Present";
            } else{
              srNos += 1;
              missingLines += srNos.toString() + ".  <b>" + reseller + "</b>  <i>(for " + region + " traffic)</i><br/>";
              dump[i][reseller] = "Not Present";
            }
          }
        dump[i].countLinesPresent =  (demandLines.length + 1) - srNos;
        if (srNos != 0 & row["Require manual checkup"] == "No"){
          dump[i].mailStatus = "Missing demand lines mail sent";
          // client.sendEmail({
          //   "From": fromMail,
          //   "To": row["Mail ID"],
          //   "Cc": ccMail,
          //   "Subject": "Missing demand lines found in Ads.txt",
          //   "HtmlBody": "<html><body style='font-size: 10pt; font-family: Arial, sans-serif;'><p>Hi "+ row["First Name"] + ",<br/><br/>" + "I hope you are doing well.<br/><br/>We were doing our monthly check on your app-ads.txt and found some demand lines missing in it.<br/>Can you please add these additional line items to your app-ads.txt domain (" + url + ") - </br><br/><br/><a style='text-decoration: none'>" + missingLines 
          //   + "</a><br/>Please let me know when it is done.<br/><br/>Thanks and Regards,</br>" + 
          //   '<table style="WIDTH: 200px; border-bottom: 1px solid" cellSpacing="0" cellPadding="0" width="400" border="0"><tbody><tr><td style="FONT-SIZE: 12px; FONT-FAMILY: Arial, sans-serif; WIDTH: 200px; COLOR: #515151; line-height:18px; border-bottom: 1px solid; padding-bottom: 5px;" width="200" colspan="2"><span style="FONT-SIZE: 17px; FONT-FAMILY: Arial; margin-bottom: 8px;"><strong>Vatsa Shah</strong></span><br><span style="FONT-SIZE: 12px; FONT-FAMILY: Arial">Operations Team</span><br><strong>LG Ad Solutions</strong><br><br><span style="COLOR: #A50034;"><strong>M.</strong> <span style="COLOR: #606060">+91 8734900979</span><br></span><span style="COLOR: #A50034; "><strong>E. </strong><span style="COLOR: #606060">vatsa@lgads.tv</span></span><br><br><a href="http://lgads.tv" target="_blank" rel="noopener"><img style="" src="https://lgads.tv/wp-content/uploads/2021/10/LGAdsolutions-logo3.png" width="200"></a></td></tr><tr> <td style="WIDTH: 200px; PADDING-BOTTOM: 15px; PADDING-TOP: 15px" vAlign="middle" align="left"><span><a href="https://www.facebook.com/LG-Ads-104021011763496" target="_blank" rel="noopener"><img border="0" width="25" src="https://lgads.tv/wp-content/uploads/2021/08/icon_Fb.png" alt="facebook icon" style="border:0; height:24px; width:24px"></a>&nbsp;</span><span><a href="https://twitter.com/LG_Ads_Platform" target="_blank" rel="noopener"><img border="0" width="25" src="https://lgads.tv/wp-content/uploads/2021/08/icon_Twitter.png" alt="twitter icon" style="border:0; height:24px; width:24px"></a>&nbsp;</span><span><a href="https://www.youtube.com/channel/UCGk2PePBwslIKg8JfQrCG3Q" target="_blank" rel="noopener"><img border="0" width="25" src="https://lgads.tv/wp-content/uploads/2021/08/icon_youtube.png" alt="youtube icon" style="border:0; height:24px; width:24px"></a>&nbsp;</span><span><a href="https://www.linkedin.com/company/lgads" target="_blank" rel="noopener"><img border="0" width="25" src="https://lgads.tv/wp-content/uploads/2021/08/icon_Linkedin.png" alt="linkedin icon" style="border:0; height:24px; width:24px"></a>&nbsp;</span></td></tr></tbody></table></body>'
          // });
        }
        else if (row["Require manual checkup"] == "Yes") {
          dump[i].mailStatus = "Requires manual checkup. Hence no mail sent.";
        }
        else {
          dump[i].mailStatus = "Pub has all the lines. Hence no mail sent.";
        }
      }
    })
    .catch((error) => {
      if(error.response === undefined){
        dump[i].HTTPstatus = "Error";
      }
      else {
        dump[i].HTTPstatus = error.response.status;
      }
      dump[i].countLinesPresent = 0;
      dump[i].mailStatus = "incorrect app-ads.txt domain mail sent";
      dump[i]["Seller id demand line"] = "app-ads.txt not found";
      for (var j=0; j<demandLines.length; j++){
        var reseller = demandLines[j]["Demand Lines"].replace("DIRECT","RESELLER");
        dump[i][reseller] = "app-ads.txt not found";
      }
      if (row["Require manual checkup"] == "Yes") {
        dump[i].mailStatus = "Requires manual checkup. Hence no mail sent.";
      }
      else {
        client.sendEmail({
          "From": fromMail,
          "To": row["Mail ID"],
          "Cc": ccMail,
          "Subject": "app-ads.txt not found",
          "HtmlBody": "<html><body style='font-size: 10pt; font-family: Arial, sans-serif;'><p>Hi "+ row["First Name"] + ",<br/><br/>" + "I hope you are doing well.<br/><br/>Can you please share with us your app-ads.txt url?<br/>We are not seeing any response to this url - " + url + "<br/><br/>" + 
          '<table style="WIDTH: 200px; border-bottom: 1px solid" cellSpacing="0" cellPadding="0" width="400" border="0"><tbody><tr><td style="FONT-SIZE: 12px; FONT-FAMILY: Arial, sans-serif; WIDTH: 200px; COLOR: #515151; line-height:18px; border-bottom: 1px solid; padding-bottom: 5px;" width="200" colspan="2"><span style="FONT-SIZE: 17px; FONT-FAMILY: Arial; margin-bottom: 8px;"><strong>Vatsa Shah</strong></span><br><span style="FONT-SIZE: 12px; FONT-FAMILY: Arial">Operations Team</span><br><strong>LG Ad Solutions</strong><br><br><span style="COLOR: #A50034;"><strong>M.</strong> <span style="COLOR: #606060">+91 8734900979</span><br></span><span style="COLOR: #A50034; "><strong>E. </strong><span style="COLOR: #606060">vatsa@lgads.tv</span></span><br><br><a href="http://lgads.tv" target="_blank" rel="noopener"><img style="" src="https://lgads.tv/wp-content/uploads/2021/10/LGAdsolutions-logo3.png" width="200"></a></td></tr><tr> <td style="WIDTH: 200px; PADDING-BOTTOM: 15px; PADDING-TOP: 15px" vAlign="middle" align="left"><span><a href="https://www.facebook.com/LG-Ads-104021011763496" target="_blank" rel="noopener"><img border="0" width="25" src="https://lgads.tv/wp-content/uploads/2021/08/icon_Fb.png" alt="facebook icon" style="border:0; height:24px; width:24px"></a>&nbsp;</span><span><a href="https://twitter.com/LG_Ads_Platform" target="_blank" rel="noopener"><img border="0" width="25" src="https://lgads.tv/wp-content/uploads/2021/08/icon_Twitter.png" alt="twitter icon" style="border:0; height:24px; width:24px"></a>&nbsp;</span><span><a href="https://www.youtube.com/channel/UCGk2PePBwslIKg8JfQrCG3Q" target="_blank" rel="noopener"><img border="0" width="25" src="https://lgads.tv/wp-content/uploads/2021/08/icon_youtube.png" alt="youtube icon" style="border:0; height:24px; width:24px"></a>&nbsp;</span><span><a href="https://www.linkedin.com/company/lgads" target="_blank" rel="noopener"><img border="0" width="25" src="https://lgads.tv/wp-content/uploads/2021/08/icon_Linkedin.png" alt="linkedin icon" style="border:0; height:24px; width:24px"></a>&nbsp;</span></td></tr></tbody></table></body>'
        });
      }
    }
    )}, {concurrency: 10000});

  const ws = XLSX.utils.json_to_sheet(dump)
  XLSX.utils.book_append_sheet(workbook,ws,"test_output")
  XLSX.writeFile(workbook,'ads_txt_crawler.xlsx')
}

main()