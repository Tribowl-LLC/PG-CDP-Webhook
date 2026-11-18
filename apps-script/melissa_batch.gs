/**
 * Melissa.com upload batches: emails a CSV of 100 customers Versium couldn't match.
 *
 * Add this file to the same Google Apps Script project as the WebRes forwarder, then:
 *   1. Project Settings -> Script Properties: add
 *        CDP_BASE_URL     the server address, e.g. https://<your-service>.onrender.com (no trailing slash)
 *        CDP_CRON_KEY     the CRON_SECRET value from Render
 *        MELISSA_EMAIL_TO (optional) who gets the CSV; defaults to the account the script runs as
 *   2. Run sendMelissaBatch once by hand and approve the permissions Google asks for.
 *   3. Triggers -> Add Trigger: sendMelissaBatch, time-driven, every hour.
 *
 * Each run asks the server for a batch. Nothing happens until 100 customers are waiting. When a batch is
 * ready it is emailed, then confirmed to the server; if the email fails, the same batch comes back next run.
 */
function sendMelissaBatch() {
  var props = PropertiesService.getScriptProperties();
  var base = props.getProperty('CDP_BASE_URL');
  var key = props.getProperty('CDP_CRON_KEY');
  if (!base || !key) throw new Error('Set CDP_BASE_URL and CDP_CRON_KEY in Script Properties');
  var to = props.getProperty('MELISSA_EMAIL_TO') || Session.getEffectiveUser().getEmail();
  var headers = { 'X-CDP-Key': key };

  var res = UrlFetchApp.fetch(base + '/cron/melissa-batch', {
    method: 'post', headers: headers, muteHttpExceptions: true
  });
  if (res.getResponseCode() !== 200) {
    throw new Error('melissa-batch answered HTTP ' + res.getResponseCode());
  }
  var batch = JSON.parse(res.getContentText());
  if (!batch.ready) {
    console.log('Melissa queue: ' + batch.waiting + ' of ' + batch.batch_size + ' waiting');
    return;
  }

  var blob = Utilities.newBlob(batch.csv, 'text/csv', batch.filename);
  GmailApp.sendEmail(to,
    'Melissa upload batch #' + batch.batch_id + ' (' + batch.customers + ' customers)',
    'Attached: ' + batch.customers + ' customers Versium could not match.\n\n' +
    'Upload ' + batch.filename + ' to Melissa.com for demographics. Keep the RecordID column in the ' +
    'results file, so the results can be matched back to each customer.\n',
    { attachments: [blob], name: 'PG CDP' });

  var ok = UrlFetchApp.fetch(base + '/cron/melissa-batch/' + batch.batch_id + '/emailed', {
    method: 'post', headers: headers, muteHttpExceptions: true
  });
  if (ok.getResponseCode() !== 200) {
    throw new Error('Emailed batch ' + batch.batch_id + ' but the confirmation answered HTTP ' +
                    ok.getResponseCode() + '; it will be emailed again next run');
  }
}
