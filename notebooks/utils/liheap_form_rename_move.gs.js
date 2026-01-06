/****************************************************
 * LIHEAP – Rename & Move uploaded file on Form submit
 * Output name: LIHEAP_YYYY_MM.xlsx
 * Move to folder: TARGET_FOLDER_ID
 ****************************************************/

// ✅ Target folder (ID only, not URL)
const TARGET_FOLDER_ID = "1vO4mKqcqjFQT3N4r4QdAK30McDrkg-NW";

// ✅ MUST match your Form question titles EXACTLY (case + spaces)
const YEAR_QUESTION_TITLE  = "Year dataset (year covered by this dataset)";
const MONTH_QUESTION_TITLE = "Month dataset (month covered by this dataset)";

// Duplicate handling: "suffix" or "overwrite"
const DUPLICATE_MODE = "suffix";

function onFormSubmit(e) {
  try {
    if (!e || !e.response) {
      throw new Error("Missing event object. Ensure trigger is set to 'On form submit'.");
    }

    const itemResponses = e.response.getItemResponses();

    // 1) Read Year & Month by question title
    const year  = getAnswerByTitle_(itemResponses, YEAR_QUESTION_TITLE);
    const month = getAnswerByTitle_(itemResponses, MONTH_QUESTION_TITLE);

    const yearNorm  = normalizeYear_(year);
    const monthNorm = normalizeMonth_(month);

    // 2) Get uploaded file ID (works only if user uploaded a file)
    const fileId = getUploadedFileId_(itemResponses);
    if (!fileId) {
      // If user selected "No" and you end the form early, there is no file upload.
      Logger.log("No file uploaded. Skipping rename/move.");
      return;
    }

    const file = DriveApp.getFileById(fileId);

    // 3) Enforce .xlsx only (optional but recommended)
    const originalName = String(file.getName()).toLowerCase();
    if (!originalName.endsWith(".xlsx")) {
      throw new Error(`Invalid file type: "${file.getName()}". Please upload an Excel .xlsx file.`);
    }

    const targetFolder = DriveApp.getFolderById(TARGET_FOLDER_ID);

    // 4) Build standardized name
    let newName = `LIHEAP_${yearNorm}_${monthNorm}.xlsx`;

    // 5) Handle duplicates
    if (DUPLICATE_MODE !== "overwrite" && fileExistsInFolder_(targetFolder, newName)) {
      const ts = Utilities.formatDate(
        new Date(),
        Session.getScriptTimeZone(),
        "yyyyMMdd-HHmmss"
      );
      newName = `LIHEAP_${yearNorm}_${monthNorm}__DUP_${ts}.xlsx`;
    }

    // 6) Rename + Move
    file.setName(newName);
    file.moveTo(targetFolder);

    Logger.log(`✅ Renamed & moved: ${newName}`);

  } catch (err) {
    Logger.log(`❌ ERROR: ${err && err.message ? err.message : err}`);
  }
}

/******************** HELPERS ********************/

function getAnswerByTitle_(itemResponses, title) {
  for (const ir of itemResponses) {
    if (ir.getItem().getTitle() === title) {
      return ir.getResponse();
    }
  }
  throw new Error(`Missing answer for question: "${title}"`);
}

function getUploadedFileId_(itemResponses) {
  for (const ir of itemResponses) {
    if (ir.getItem().getType() === FormApp.ItemType.FILE_UPLOAD) {
      const resp = ir.getResponse(); // array of file IDs
      if (Array.isArray(resp) && resp.length > 0) return resp[0];
    }
  }
  return null;
}

function normalizeYear_(year) {
  const y = String(year).trim();
  if (!/^\d{4}$/.test(y)) {
    throw new Error(`Invalid year: "${year}" (expected YYYY)`);
  }
  return y;
}

function normalizeMonth_(month) {
  // Your dropdown values are likely "01", "02", ..., "12"
  const m = String(month).trim();
  if (!/^\d{1,2}$/.test(m)) {
    throw new Error(`Invalid month: "${month}" (expected 01–12)`);
  }
  const n = parseInt(m, 10);
  if (n < 1 || n > 12) {
    throw new Error(`Invalid month: "${month}" (expected 01–12)`);
  }
  return String(n).padStart(2, "0");
}

function fileExistsInFolder_(folder, fileName) {
  return folder.getFilesByName(fileName).hasNext();
}