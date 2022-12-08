<%@page contentType="text/html" pageEncoding="UTF-8"%>
<%@ taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c" %>
<%@ taglib prefix="fn" uri="http://java.sun.com/jsp/jstl/functions" %>
<!DOCTYPE html>
<html>

<head>
<title>${caption}</title>

<style>
table { margin: 1em auto; border-collapse: collapse }
caption { padding: 1em; font-size: larger; font-weight: bold }
th { font-weight: normal; font-size: larger }
tbody > tr:hover { background: #eaf3fe }
th, td { padding-left: 2ch; text-align: center } th:first-child, td:first-child { text-align: left }

a { color: inherit; text-decoration: none } a:hover { text-decoration: underline }
</style>
</head>

<body>

<table>

<caption>documents containing: <c:out value="${caption}"/></caption>

<thead>
<tr>
	<th>Title</th>
	<c:choose>
		<c:when test="${fn:length(ngrams) eq 1}">
			<th>count</th>
		</c:when>
		<c:otherwise>
			<c:forEach var="ngram" items="${ngrams}">
				<th><c:out value="${ngram}"/></th>
			</c:forEach>
		</c:otherwise>
	</c:choose>
</tr>
</thead>

<tbody>
<c:forEach var="entry" items="${instances}">
<tr>
<td><a href="http://www.legislation.gov.uk/<c:out value="${entry.key}"/>" target="blank"><c:out value="${titles[entry.key]}"/></a></td>
<c:forEach var="e2" items="${entry.value}">
<td><c:out value="${e2.value}"/></td>
</c:forEach>
</tr>
</c:forEach>
</tbody>

</table>

</body>

</html>
