package com.cl.data.hbase.util;


public class BuildCsvString {
    private static UserSettings userSettings;
    private static boolean useCustomRecordDelimiter;

    static {
        useCustomRecordDelimiter = false;
        userSettings = new UserSettings();
    }

    public static String writeRecord(String[] paramArrayOfString, boolean paramBoolean) {
        if ((paramArrayOfString == null) || (paramArrayOfString.length <= 0)) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < paramArrayOfString.length; i++) {
            boolean firstColumn = i == 0;
            write(firstColumn, paramArrayOfString[i], paramBoolean, sb);
        }
        return sb.toString();
    }

    public static String build(String[] paramArrayOfString, char delimiter) {
        userSettings.Delimiter = delimiter;
        return writeRecord(paramArrayOfString, false);
    }

    public static void write(boolean firstColumn, String paramString, boolean paramBoolean, StringBuilder sb) {
        if (paramString == null) {
            paramString = "";
        }
        if (!(firstColumn)) {
            sb.append(userSettings.Delimiter);
        }
        boolean bool = userSettings.ForceQualifier;
        if ((!(paramBoolean)) && (paramString.length() > 0)) {
            paramString = paramString.trim();
        }
        if ((!(bool)) && (userSettings.UseTextQualifier)
                && (((paramString.indexOf(userSettings.TextQualifier) > -1)
                || (paramString.indexOf(userSettings.Delimiter) > -1)
                || ((!(useCustomRecordDelimiter))
                && (((paramString.indexOf(10) > -1) || (paramString.indexOf(13) > -1))))
                || ((useCustomRecordDelimiter)
                && (paramString.indexOf(userSettings.RecordDelimiter) > -1))
                || ((firstColumn) && (paramString.length() > 0)
                && (paramString.charAt(0) == userSettings.Comment))
                || ((firstColumn) && (paramString.length() == 0))))) {
            bool = true;
        }
        if ((userSettings.UseTextQualifier) && (!(bool)) && (paramString.length() > 0) && (paramBoolean)) {
            int i = paramString.charAt(0);
            if ((i == 32) || (i == 9)) {
                bool = true;
            }
            if ((!(bool)) && (paramString.length() > 1)) {
                int j = paramString.charAt(paramString.length() - 1);
                if ((j == 32) || (j == 9)) {
                    bool = true;
                }
            }
        }
        if (bool) {
            sb.append(userSettings.TextQualifier);
            if (userSettings.EscapeMode == 2) {
                paramString = replace(paramString, "\\", "\\\\");
                paramString = replace(paramString, "" + userSettings.TextQualifier,
                        "\\" + userSettings.TextQualifier);
            } else {
                paramString = replace(paramString, "" + userSettings.TextQualifier,
                        "" + userSettings.TextQualifier + userSettings.TextQualifier);
            }
        } else if (userSettings.EscapeMode == 2) {
            paramString = replace(paramString, "\\", "\\\\");
            paramString = replace(paramString, "" + userSettings.Delimiter, "\\" + userSettings.Delimiter);
            if (useCustomRecordDelimiter) {
                paramString = replace(paramString, "" + userSettings.RecordDelimiter,
                        "\\" + userSettings.RecordDelimiter);
            } else {
                paramString = replace(paramString, "\r", "\\\r");
                paramString = replace(paramString, "\n", "\\\n");
            }
            if ((firstColumn) && (paramString.length() > 0) && (paramString.charAt(0) == userSettings.Comment)) {
                if (paramString.length() > 1) {
                    paramString = "\\" + userSettings.Comment + paramString.substring(1);
                } else {
                    paramString = "\\" + userSettings.Comment;
                }
            }
        }
        sb.append(paramString);
        if (bool) {
            sb.append(userSettings.TextQualifier);
        }
        firstColumn = false;
    }

    public static String replace(String paramString1, String paramString2, String paramString3) {
        int i = paramString2.length();
        int j = paramString1.indexOf(paramString2);
        if (j > -1) {
            StringBuffer localStringBuffer = new StringBuffer();
            int k = 0;
            while (j != -1) {
                localStringBuffer.append(paramString1.substring(k, j));
                localStringBuffer.append(paramString3);
                k = j + i;
                j = paramString1.indexOf(paramString2, k);
            }
            localStringBuffer.append(paramString1.substring(k));
            return localStringBuffer.toString();
        }
        return paramString1;
    }

    private static class UserSettings {
        public char TextQualifier = '"';
        public boolean UseTextQualifier = true;
        public char Delimiter = ',';
        public char RecordDelimiter = '\0';
        public char Comment = '#';
        public int EscapeMode = 1;
        public boolean ForceQualifier = false;
    }

    public static void main(String[] args) {
        String[] array = new String[]{"a", "b,s,huo", "c"};
        String[] array2 = new String[]{"4297854405515850", "", "", "", ""};
        for (int i = 0; i < 50; i++) {
            String str = BuildCsvString.build(array, ',');
            System.out.println(str);
            String str2 = BuildCsvString.build(array2, ',');
            System.out.println(str2);
        }
    }
}
