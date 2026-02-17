// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::ast::{BinaryOperator, CommentObject, Expr, Statement};
use crate::dialect::Dialect;
use crate::keywords::Keyword;
use crate::parser::{Parser, ParserError};
use crate::tokenizer::Token;

#[derive(Debug)]
pub struct PostgreSqlDialect {}

impl Dialect for PostgreSqlDialect {
    fn identifier_quote_style(&self, _identifier: &str) -> Option<char> {
        Some('"')
    }

    fn is_identifier_start(&self, ch: char) -> bool {
        // See https://www.postgresql.org/docs/11/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS
        // We don't yet support identifiers beginning with "letters with
        // diacritical marks"
        ch.is_alphabetic() || ch == '_'
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        ch.is_alphabetic() || ch.is_ascii_digit() || ch == '$' || ch == '_'
    }

    fn parse_statement(&self, parser: &mut Parser) -> Option<Result<Statement, ParserError>> {
        if parser.parse_keyword(Keyword::COMMENT) {
            Some(parse_comment(parser))
        } else {
            None
        }
    }

    fn supports_filter_during_aggregation(&self) -> bool {
        true
    }

    fn supports_group_by_expr(&self) -> bool {
        true
    }

    fn get_next_precedence(&self, parser: &Parser) -> Option<Result<u8, ParserError>> {
        let tok0 = parser.peek_nth_token(0);
        // Detect `-|-` (range adjacency operator) as three tokens: Minus, Pipe, Minus
        if tok0.token == Token::Minus {
            let tok1 = parser.peek_nth_token(1);
            let tok2 = parser.peek_nth_token(2);
            if tok1.token == Token::Pipe && tok2.token == Token::Minus {
                return Some(Ok(50)); // Same precedence as other range operators (&&, @>, <@)
            }
        }
        // Detect `<->` (distance operator) as two tokens: Lt, Arrow
        if tok0.token == Token::Lt {
            let tok1 = parser.peek_nth_token(1);
            if tok1.token == Token::Arrow {
                return Some(Ok(50));
            }
        }
        None
    }

    fn parse_infix(
        &self,
        parser: &mut Parser,
        expr: &Expr,
        _precedence: u8,
    ) -> Option<Result<Expr, ParserError>> {
        let tok0 = parser.peek_nth_token(0);
        // Parse `-|-` (range adjacency operator) as three tokens: Minus, Pipe, Minus
        if tok0.token == Token::Minus {
            let tok1 = parser.peek_nth_token(1);
            let tok2 = parser.peek_nth_token(2);
            if tok1.token == Token::Pipe && tok2.token == Token::Minus {
                parser.next_token(); // consume `-`
                parser.next_token(); // consume `|`
                parser.next_token(); // consume `-`
                return Some(
                    parser
                        .parse_subexpr(50)
                        .map(|right| Expr::BinaryOp {
                            left: Box::new(expr.clone()),
                            op: BinaryOperator::PGAdjacentTo,
                            right: Box::new(right),
                        }),
                );
            }
        }
        // Parse `<->` (distance operator) as two tokens: Lt, Arrow
        if tok0.token == Token::Lt {
            let tok1 = parser.peek_nth_token(1);
            if tok1.token == Token::Arrow {
                parser.next_token(); // consume `<`
                parser.next_token(); // consume `->`
                return Some(
                    parser
                        .parse_subexpr(50)
                        .map(|right| Expr::BinaryOp {
                            left: Box::new(expr.clone()),
                            op: BinaryOperator::PGDistance,
                            right: Box::new(right),
                        }),
                );
            }
        }
        None
    }
}

pub fn parse_comment(parser: &mut Parser) -> Result<Statement, ParserError> {
    let if_exists = parser.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);

    parser.expect_keyword(Keyword::ON)?;
    let token = parser.next_token();

    let (object_type, object_name) = match token.token {
        Token::Word(w) if w.keyword == Keyword::COLUMN => {
            let object_name = parser.parse_object_name(false)?;
            (CommentObject::Column, object_name)
        }
        Token::Word(w) if w.keyword == Keyword::TABLE => {
            let object_name = parser.parse_object_name(false)?;
            (CommentObject::Table, object_name)
        }
        Token::Word(w) => {
            let object_name = parser.parse_object_name(false)?;
            (CommentObject::Other(w.value.to_uppercase()), object_name)
        }
        _ => parser.expected("comment object_type", token)?,
    };

    parser.expect_keyword(Keyword::IS)?;
    let comment = if parser.parse_keyword(Keyword::NULL) {
        None
    } else {
        Some(parser.parse_literal_string()?)
    };
    Ok(Statement::Comment {
        object_type,
        object_name,
        comment,
        if_exists,
    })
}
