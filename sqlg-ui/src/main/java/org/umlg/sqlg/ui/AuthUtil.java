package org.umlg.sqlg.ui;

import com.auth0.jwt.JWT;
import com.auth0.jwt.JWTVerifier;
import com.auth0.jwt.algorithms.Algorithm;
import com.auth0.jwt.exceptions.JWTVerificationException;
import com.auth0.jwt.interfaces.DecodedJWT;

public class AuthUtil {

    static final String SQLG_TOKEN = "SqlgToken";
    private static final Algorithm ALGORITHM = Algorithm.HMAC256("secret");
    private static final String ISSUER = "sqlg";
    private static final JWTVerifier JWT_VERIFIER = JWT.require(ALGORITHM)
            .withIssuer(ISSUER)
            .build();

    public static String generateToken(String username) {
        return JWT.create()
                .withClaim("username", username)
                .withIssuer(ISSUER)
                .sign(ALGORITHM);
    }


    public static DecodedJWT validToken(String token) {
        try {
            return  JWT_VERIFIER.verify(token);
        } catch (JWTVerificationException exception) {
            return null;
        }
    }
}
