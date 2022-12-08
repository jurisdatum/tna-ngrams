<%@page contentType="text/html" pageEncoding="UTF-8"%>
<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core" %>
<%@ taglib prefix="ng" uri="/WEB-INF/ngrams.tld" %>
<!DOCTYPE html>
<html>

<head>
<title>${components}</title>
<link rel="stylesheet" href="/ngrams.css">
<style> th { text-align: left }  td { vertical-align: top } </style>
</head>

<body>

<table>

<caption>n-grams containing: ${components}</caption>

<tr>
<c:forEach var="entry" items="${ngrams}">
	<th>
		<c:if test="${ ! empty entry.value }"><c:out value="${entry.key}"/>-grams</c:if>
	</th>
</c:forEach>
</tr>

<tr>
<c:forEach var="entry" items="${ngrams}">
	<td>
	<c:forEach var="ngram" items="${entry.value}">
		<div>
			<a href="${ng:link(legType, ngram.key, ngType)}"><c:out value="${ngram.key}"/> (<c:out value="${ngram.value}"/>)</a>
		</div>
	</c:forEach>
	</td>
</c:forEach>
</tr>
</table>

</body>

</html>
