function onOpen() { 
 SpreadsheetApp.getUi().createMenu('Digizag') 
 .addItem('Split by Account Manager', 'splitByAccountManager') 
 .addToUi(); // Removed Sync item 
} 
 
function splitByAccountManager() { 
 const ss = SpreadsheetApp.getActiveSpreadsheet(); 
 const sourceSheet = ss.getSheetByName("All Payments"); 
 const sourceData = sourceSheet.getDataRange(); 
 const allValues = sourceData.getValues(); 
 const headers = allValues[0]; 
 const rows = allValues.slice(1); 
 
 const allFormats = { 
 fontWeights: sourceData.getFontWeights(), 
 fontColors: sourceData.getFontColors(), 
 backgrounds: sourceData.getBackgrounds(), 
 fontSizes: sourceData.getFontSizes(), 
 fontStyles: sourceData.getFontStyles(), 
 hAlign: sourceData.getHorizontalAlignments(), 
 vAlign: sourceData.getVerticalAlignments(), 
 wraps: sourceData.getWraps(), 
 numberFormats: sourceData.getNumberFormats() 
 }; 
 
 const colWidths = headers.map((_, i) => sourceSheet.getColumnWidth(i + 1)); 
 
 const grouped = {}; 
 rows.forEach((row, i) => { 
 if (row.every(cell => cell === "")) return; 
 let manager = row[0] || "No Manager"; 
 if (manager === '#N/A' || manager.toString().trim() === '') manager = "No Manager"; 
 if (!grouped[manager]) grouped[manager] = []; 
 grouped[manager].push({ rowData: row, formatIndex: i + 1 }); 
 }); 
 
 for (let manager in grouped) { 
 const sheetName = manager.substring(0, 99); 
 let sheet = ss.getSheetByName(sheetName) || ss.insertSheet(sheetName); 
 sheet.clear(); 
 
 const fullRows = [headers, ...grouped[manager].map(obj => obj.rowData)]; 
 sheet.getRange(1, 1, fullRows.length, headers.length).setValues(fullRows); 
 
 for (let i = 0; i < fullRows.length; i++) { 
 const srcIndex = i === 0 ? 0 : grouped[manager][i - 1].formatIndex; 
 const range = sheet.getRange(i + 1, 1, 1, headers.length); 
 range.setFontWeights([allFormats.fontWeights[srcIndex]]); 
 range.setFontColors([allFormats.fontColors[srcIndex]]); 
 range.setBackgrounds([allFormats.backgrounds[srcIndex]]); 
 range.setFontSizes([allFormats.fontSizes[srcIndex]]); 
 range.setFontStyles([allFormats.fontStyles[srcIndex]]); 
 range.setHorizontalAlignments([allFormats.hAlign[srcIndex]]); 
 range.setVerticalAlignments([allFormats.vAlign[srcIndex]]); 
 range.setWraps([allFormats.wraps[srcIndex]]); 
 range.setNumberFormats([allFormats.numberFormats[srcIndex]]); 
 } 
 
 colWidths.forEach((w, i) => sheet.setColumnWidth(i + 1, w)); 
 } 
}