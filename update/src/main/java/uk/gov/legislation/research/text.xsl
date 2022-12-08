<?xml version="1.0" encoding="utf-8"?>

<xsl:stylesheet version="2.0"
	xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
	xmlns:xs="http://www.w3.org/2001/XMLSchema"
	xpath-default-namespace="http://www.legislation.gov.uk/namespaces/legislation"
	xmlns:ukm="http://www.legislation.gov.uk/namespaces/metadata"
	xmlns:html="http://www.w3.org/1999/xhtml"
	xmlns:local="http://www.mangiafico.com/tna/ngrams"
>

<xsl:param name="titles" select="true()" as="xs:boolean" />
<xsl:param name="explanatory-notes" select="false()" as="xs:boolean" />
<xsl:param name="commentaries" select="false()" as="xs:boolean" />

<xsl:output method="text" encoding="utf-8" />
<xsl:strip-space elements="*" />


<xsl:template match="ukm:Metadata" />

<xsl:template match="AppendText">
	<xsl:apply-templates />
	<xsl:text>&#13;</xsl:text>
</xsl:template>

<xsl:template match="BlockAmendment">
	<xsl:text>&#8220;</xsl:text>
	<xsl:apply-templates />
	<xsl:if test="not(following-sibling::*[1][self::AppendText])">
		<xsl:text>&#13;</xsl:text>
	</xsl:if>
</xsl:template>

<xsl:template match="DateOfEnactment" />

<xsl:template match="ExplanatoryNotes">
	<xsl:if test="$explanatory-notes"><xsl:apply-templates /></xsl:if>
</xsl:template>

<xsl:template match="LongTitle">
	<xsl:apply-templates />
	<xsl:text>&#13;</xsl:text>
</xsl:template>

<xsl:template match="Number" />

<xsl:template match="P1">
<xsl:if test="not($titles)">
	<xsl:text>&#13;</xsl:text>
</xsl:if>
	<xsl:apply-templates />
</xsl:template>

<xsl:template match="P1group">
<xsl:if test="$titles">
	<xsl:text>&#13;</xsl:text>
</xsl:if>
	<xsl:apply-templates />
</xsl:template>

<xsl:template match="Pblock">
<xsl:if test="$titles">
	<xsl:text>&#13;</xsl:text>
</xsl:if>
	<xsl:apply-templates />
</xsl:template>

<xsl:template match="Pnumber" />

<xsl:template match="SubjectInformation | SiftedDate | MadeDate | LaidDate | ComingIntoForce" />

<xsl:template match="Reference" /><!-- Schedule/Reference? -->

<xsl:template match="Resources" />

<xsl:template match="BlockAmendment//Text">
	<xsl:apply-templates />
	<xsl:variable name="block-amendment" select="ancestor::BlockAmendment[1]" as="element()" />
	<xsl:variable name="texts" select="$block-amendment/descendant::Text" />
	<xsl:choose>
		<xsl:when test="generate-id($texts[last()]) = generate-id()">
			<xsl:text>&#8221;</xsl:text>
		</xsl:when>
		<xsl:otherwise>
			<xsl:text>&#13;</xsl:text>
		</xsl:otherwise>
	</xsl:choose>
</xsl:template>
<xsl:template match="Text">
	<xsl:apply-templates />
	<xsl:text>&#13;</xsl:text>
</xsl:template>

<xsl:template match="Title">
<xsl:if test="$titles">
	<xsl:apply-templates />
	<xsl:text>&#13;</xsl:text>
</xsl:if>
</xsl:template>


<!-- add line break after -->
<xsl:template match="PersonName | JobTitle | DateSigned">
	<xsl:apply-templates />
	<xsl:text>&#13;</xsl:text>
</xsl:template>


<xsl:template match="Versions" />

<xsl:template match="Commentaries">
	<xsl:if test="$commentaries"><xsl:apply-templates /></xsl:if>
</xsl:template>

<!-- html -->

<xsl:template match="html:tr">
	<xsl:apply-templates />
	<xsl:text>&#13;</xsl:text>
</xsl:template>
<xsl:template match="html:th">
	<xsl:if test="preceding-sibling::html:th">
		<xsl:text>&#9;</xsl:text>
	</xsl:if>
	<xsl:apply-templates />
</xsl:template>
<xsl:template match="html:td">
	<xsl:if test="preceding-sibling::html:td">
		<xsl:text>&#9;</xsl:text>
	</xsl:if>
	<xsl:apply-templates />
</xsl:template>


<!-- text -->

<xsl:function name="local:is-uppercase" as="xs:boolean">
	<xsl:param name="text" as="text()" />
	<xsl:sequence select="exists($text/ancestor::Uppercase)" />
</xsl:function>

<xsl:template match="text()">
	<xsl:if test="substring(., 1, 1) = ' '"><xsl:text> </xsl:text></xsl:if>
	<xsl:choose>
		<xsl:when test="local:is-uppercase(.)">
			<xsl:value-of select="upper-case(normalize-space())" />
		</xsl:when>
		<xsl:otherwise>
			<xsl:value-of select="normalize-space()" />
		</xsl:otherwise>
	</xsl:choose>
	<xsl:if test="substring(., string-length(.), 1) = ' '"><xsl:text> </xsl:text></xsl:if>
</xsl:template>

</xsl:stylesheet>
