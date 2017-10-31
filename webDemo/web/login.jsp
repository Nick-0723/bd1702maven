<%@ taglib prefix="s" uri="/struts-tags" %>
<%--
  Created by IntelliJ IDEA.
  User: Nick
  Date: 2017/10/31
  Time: 14:26
  To change this template use File | Settings | File Templates.
--%>
<%@ page contentType="text/html;charset=UTF-8" language="java" %>
<html>
<head>
    <title>login</title>
</head>
<body>
    <s:form action="login">
        <s:textfield name="username" key="user"/>
        <s:textfield name="password" key="pass"/>
        <s:submit key="login"/>
    </s:form>
</body>
</html>
