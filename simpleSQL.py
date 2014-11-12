# simpleSQL.py
#
# simple demo of using the parsing library to do simple-minded SQL parsing
# could be extended to include where clauses etc.
#
# Copyright (c) 2003, Paul McGuire
#
# Modified by Adeola Bannis, 2014
# Added LIMIT and OFFSET clauses
#
from pyparsing import Literal, CaselessLiteral, Word,  delimitedList, Optional, \
    Combine, Group, alphas, nums, alphanums, ParseException, Forward, oneOf, quotedString, \
    ZeroOrMore, restOfLine, Keyword

def test( str ):
    print str,"->"
    try:
        tokens = simpleSQL.parseString( str )
        print "tokens = ",        tokens
        print "tokens.columns =", tokens.columns
        print "tokens.table =",  tokens.table
        print "tokens.where =", tokens.where
        print "tokens.limit = ", tokens.limit
        print "tokens.offset = ", tokens.offset
    except ParseException, err:
        print " "*err.loc + "^\n" + err.msg
        print err
    print


# define SQL tokens
selectStmt = Forward()
selectToken = Keyword("select", caseless=True)

ident          = Word( alphas, alphanums + "_$" ).setName("identifier")
columnName     = delimitedList( ident, ".", combine=True ) 
columnNameList = Group( delimitedList( columnName ) )


tableName      =  delimitedList( ident, ".", combine=True ) 

whereExpression = Forward()
and_ = Keyword("and", caseless=True)
or_ = Keyword("or", caseless=True)
in_ = Keyword("in", caseless=True)

E = CaselessLiteral("E")
binop = oneOf("= != < > >= <= eq ne lt le gt ge", caseless=True)
arithSign = Word("+-",exact=1)
realNum = Combine( Optional(arithSign) + ( Word( nums ) + "." + Optional( Word(nums) )  |
                                                         ( "." + Word(nums) ) ) + 
            Optional( E + Optional(arithSign) + Word(nums) ) )
intNum = Combine( Optional(arithSign) + Word( nums ) + 
            Optional( E + Optional("+") + Word(nums) ) )

columnRval = realNum | intNum | quotedString | columnName # need to add support for alg expressions
whereCondition = Group(
    ( columnName + binop + columnRval ) |
    ( columnName + in_ + "(" + delimitedList( columnRval ) + ")" ) |
    ( columnName + in_ + "(" + selectStmt + ")" ) |
    ( "(" + whereExpression + ")" )
    )
whereExpression << whereCondition + ZeroOrMore( ( and_ | or_ ) + whereExpression ) 

# define the grammar
selectStmt      << ( selectToken + 
                   ( '*' | columnNameList ).setResultsName( "columns" ) + 
                   Optional( Group ( CaselessLiteral("from") + tableName), "").setResultsName("table")+
                   Optional( Group( CaselessLiteral("where") + whereExpression ), "" ).setResultsName("where") +
                   Optional( Group( CaselessLiteral("limit") + intNum), "").setResultsName("limit") +
                   Optional( Group( CaselessLiteral("offset") + intNum), "").setResultsName("offset") )

simpleSQL = selectStmt

# define Oracle comment format, and ignore them
oracleSqlComment = "--" + restOfLine
simpleSQL.ignore( oracleSqlComment )

test("select * where quantity='current'")
test("select * from bms where quantity='voltage'")
test("select panel_name, ts where room='1451' and building='melnitz'")
test("select * from bms where quantity='voltage' limit 10")
test("select room, panel_name from bms where quantity='current' offset 10")
test("select ts, val limit 10 offset 20")
