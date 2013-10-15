package SQLMRserver;

import java.io.IOException;

import hy.TableManager.SqlParser.SqlParser;
import hy.TableManager.SqlParser.SqlStatement.CreateTableStmt;
import hy.TableManager.SqlParser.SqlStatement.SelectStmt;
import hy.TableManager.SqlParser.SqlStatement.TableSchema;
import hy.TableManager.SqlParser.SqlStatement.AlterTableOption;
import hy.TableManager.SqlParser.SqlStatement.AlterTableStmt;
import hy.TableManager.TableManager;


public class StatementParser {
	private String sqlQuery;
	private SqlParser sqlParser;
	private String referencedColumns;
	
	public StatementParser (String sqlQuery) throws Exception {
		this.sqlQuery = sqlQuery;
		this.sqlParser = new SqlParser(sqlQuery);
		boolean ret = sqlParser.parse();
		System.out.println("sqlQuery: " + sqlQuery);
		if (ret) {	
			for (int i = 0; i < sqlParser.getSqlStatementListSize(); i++) {
				switch (sqlParser.getSqlStatementTypeById(i)) {
				case CREATE_TABLE_STMT:
					CreateTableStmt createTableStmt = sqlParser
							.getCreateTableStmtById(i);
					TableSchema tableSchema = new TableSchema(createTableStmt);
					try {
						TableManager.writeTableSchemeIntoFile(tableSchema);
					} catch (IOException e) {
						e.printStackTrace();
					}
					break;
				case SELECT_STMT:
					SelectStmt selectStmt = sqlParser.getSelectStmtById(i);
					referencedColumns = TableManager.getSelectListFromSelectStmt(selectStmt);
					break;				
				case ALTER_TABLE_STMT:
                    AlterTableStmt alterTableStmt = sqlParser.getAlterTableStmtById(i);
                    for(int j = 0; j < alterTableStmt.getAlterTableOptionListSize(); j++) {
                        AlterTableOption option = alterTableStmt.getAlterTableOptionById(j);
                        option.getAlterTableOptionType();
                    }
                    break;
				default:

					break;
				}
			}
		} else {
			throw new Exception("parse error!");
		}
		
	}
	
	
	public String getParseResult() {
		return referencedColumns;
	}
}