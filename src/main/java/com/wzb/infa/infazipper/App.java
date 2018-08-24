package com.wzb.infa.infazipper;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.sql.SQLException;
import java.text.CharacterIterator;
import java.text.SimpleDateFormat;
import java.text.StringCharacterIterator;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.log4j.Logger;
import org.dom4j.DocumentException;

import com.wzb.infa.dbutils.InfaUtils;
import com.wzb.infa.exceptions.CheckTableExistException;
import com.wzb.infa.exceptions.NoPrimaryKeyException;
import com.wzb.infa.exceptions.UnsupportedDatatypeException;
import com.wzb.infa.properties.InfaProperty;

/**
 * Hello world!1
 *
 */
public class App {

    private final InfaProperty infaProperty = InfaProperty.getInstance();
    private final Logger log = Logger.getLogger(App.class);
    // 依赖infaProperty，所以必须在infaProperty的“自定义配置”之后在获取实例
    private InfaUtils infaUtil = null;

    public static void main(String[] args) throws DocumentException, IOException {

        App app = new App();
        if (args.length > 0) {
            // 自定义配置
            app.infaProperty.addInfaProperty(args[0]);
        }
        // 在infaProperty的“自定义配置”之后infaUtil在获取实例
        app.infaUtil = new InfaUtils();

        SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
        String beginDate = sdf.format(new Date());
        app.run();
        String endDate = sdf.format(new Date());
        System.out.println(beginDate + "-------" + endDate);
        // 释放资源
        app.infaUtil.dbUtil.release();
        app.log.info("quit 0");
    }

    private void run() throws IOException {
        // 定义参数
        String mappingType = infaProperty.getProperty("mapping.type", "-1");
        HashSet<String> tableList = new HashSet<>();// 保存要生成的表清单
        BufferedReader tableListReader = null;// 读取要生成的表名清单的Reader
        BufferedWriter errorTableLogWriter = null;// 生成报错日志记录

        String table;
        try {
            // 初始化错误日志记录文件
            File errorTableLogTo = new File(infaProperty.getProperty("work.dir")
                    + infaProperty.getProperty("tables.err_tables", "err_tables.txt"));

            if (!errorTableLogTo.exists()) {
                //errorTableLogTo.createNewFile();
            }

            errorTableLogWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(errorTableLogTo)));

            tableListReader = new BufferedReader(new InputStreamReader(
                    new FileInputStream(infaProperty.getProperty("work.dir") + infaProperty.getProperty("tables"))));

            while ((table = tableListReader.readLine()) != null) {
                tableList.add(table.trim().toUpperCase());
            }
            // 制作XML
            int size = tableList.size();
            int errorSize = 0;
            int tableCount = 0;// 已经生成XML的表数量
            infaUtil.xmlFileChange(0);

            for (Iterator<String> it = tableList.iterator(); it.hasNext();) {
                table = it.next();
                System.out.print("Makeing xml for " + StringPadder.rightPad(table, "-", 30) + "("
                        + (tableCount + errorSize + 1) + "/" + size + ")");
                try {
                    switch (mappingType) {
                        case "1":
                            infaUtil.makeZipperXML(table);
                            break;
                        case "2":
                            infaUtil.makeAddXML(table);
                            break;
                        default:
                            infaUtil.makeTruncateThenDeleteXML(table);
                            break;
                    }
                    System.out.println("success!");
                    tableCount++;
                } catch (NoPrimaryKeyException | CheckTableExistException | UnsupportedDatatypeException
                        | SQLException e) {
                    errorSize++;
                    errorTableLogWriter.write(table + " exception info:" + e.getMessage());
                    errorTableLogWriter.newLine();
                    errorTableLogWriter.flush();
                    System.out.println(" exception info:" + e.getMessage());
                }
                if ((tableCount % Integer.parseInt(infaProperty.getProperty("xml.output.mappings", "20"))) == 0) {
                    infaUtil.xmlFileWrite(".");
                    int fileCount = Math.floorDiv(tableCount,
                            Integer.parseInt(infaProperty.getProperty("xml.output.mappings", "20")));
                    infaUtil.xmlFileChange(fileCount);
                }
            }
            infaUtil.xmlFileWrite(".");
        } catch (FileNotFoundException e) {
            log.warn(e.getMessage());
        } catch (UnsupportedEncodingException e) {
            log.warn(e.getMessage());
        } catch (IOException e) {
            log.warn(e.getMessage());
        } catch (NullPointerException e) {
            infaUtil.xmlFileWrite(".");
        } finally {
            try {
                if (tableListReader != null) {
                    tableListReader.close();
                }
                if (errorTableLogWriter != null) {
                    errorTableLogWriter.close();
                }
            } catch (IOException e) {
                log.warn(e.getMessage());
            }
        }
    }

    static class StringPadder {

        private static StringBuffer strb;
        private static StringCharacterIterator sci;

        public static String rightPad(String stringToPad, String padder, int size) {
            if (padder.length() == 0) {
                return stringToPad;
            }
            strb = new StringBuffer(stringToPad);
            sci = new StringCharacterIterator(padder);

            while (strb.length() < size) {
                for (char ch = sci.first(); ch != CharacterIterator.DONE; ch = sci.next()) {
                    if (strb.length() < size) {
                        strb.append(String.valueOf(ch));
                    }
                }
            }
            return strb.toString();
        }
    }
}
