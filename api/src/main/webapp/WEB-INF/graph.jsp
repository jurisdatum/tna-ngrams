<!doctype html>

<html>
<head>
<style>

body { font-family: Helvetica, sans-serif }

h1 { margin-bottom: 6pt; text-align: center }
h2 { margin-top: 6pt; text-align: center }

td { max-width: 12ch; min-width: 6ch }

.axis path,
.axis line {
	fill: none;
	stroke: #000;
	shape-rendering: crispEdges
}

.line { fill: none; stroke-width: 1.5px }


</style>

</head>
<body>

<h1>${ngrams}</h1>
<h2>${dates}</h2>

<script src="/lib/d3.js"></script>
<script>

var margin = {top: 20, right: 80, bottom: 30, left: 50},
	width = 960 - margin.left - margin.right,
	height = 500 - margin.top - margin.bottom;

var x = d3.scale.linear()
	.range([0, width]);

var y = d3.scale.linear()
	.range([height, 0]);

var color = d3.scale.category10();

var xAxis = d3.svg.axis()
	.scale(x)
	.orient("bottom")
	.tickFormat(d3.format("d"));

var yAxis = d3.svg.axis()
	.scale(y)
	.orient("left");

var yLabel = d3.select('body').append('div')
	.style({'position': 'absolute', 'top': '18em', 'left': '2ch', 'width': '14ch', 'text-align': 'center'})
	.html('Frequency<br><br>1 = the average frequency of the average n-gram in the average year');

var line = d3.svg.line()
	.interpolate("basis")
	.x(function(d) { return x(d.year); })
	.y(function(d) { return y(d.scaled); });

var svg = d3.select("body")
	.append("div")
		.style({'text-align': 'center', 'font-size': 'smaller'})
	.append("svg")
		.attr("width", width + margin.left + margin.right)
		.attr("height", height + margin.top + margin.bottom)
	.append("g")
		.attr("transform", "translate(" + margin.left + "," + margin.top + ")");

var magic = d3.select('body').append('table')
	.style({'position': 'absolute', 'top': '10%', 'left': '85%'});


var data = ${data};

magic.append('tr').append('th').attr('colspan', '2').text('Counts');
magic.append('tr').attr('class', 'year')
	.html('<td style="text-align:right">year</td><td class="value"></td>');

color.domain(d3.keys(data[0].counts));

var counts = color.domain().map(function(name) { return {
	name: name,
	values: data.map(function(d) {
		var n = name.split(' ').length;
		var scale = d.scales[n.toString()];
		return {
			year: d.year,
			count: d.counts[name],
			scaled: scale === 0 ? 0 : d.counts[name] / scale
		}; 
	})
}; });

console.log(counts);

x.domain(d3.extent(data, function(d) { return d.year; }));

y.domain([
	d3.min(counts, function(c) { return d3.min(c.values, function(v) { return v.scaled; }); }),
	d3.max(counts, function(c) { return d3.max(c.values, function(v) { return v.scaled; }); })
]);

svg.append("g")
	.attr("class", "x axis")
	.attr("transform", "translate(0," + height + ")")
	.call(xAxis);

svg.append("g")
	.attr("class", "y axis")
	.call(yAxis);

var count = svg.selectAll(".count")
	.data(counts)
	.enter().append("g")
		.attr("class", "count");

count.append("path")
	.attr("class", "line")
	.attr("d", function(d) { return line(d.values); })
	.style("stroke", function(d) { return color(d.name); });

count.append("text")
	.datum(function(d) { return {name: d.name, value: d.values[d.values.length - 1]}; })
	.attr("transform", function(d) { return "translate(" + x(d.value.year) + "," + y(d.value.scaled) + ")"; })
	.attr("x", 3)
	.attr("dy", ".35em")
	.text(function(d) { return d.name; });

svg.append("rect")
	.attr("width", width)
	.attr("height", height)
	.style("fill", "none")
	.style("pointer-events", "all")
	.on("mouseover", function() { focus.style("display", null) })
	.on("mouseout", function() {
		focus.style("display", 'none')
		magic.selectAll('.value, .count').text('');
	})
	.on("mousemove", function() {
		var coords = d3.mouse(this);
		var year = Math.round(x.invert(coords[0]));
		focus.select(".line")
			.attr("transform", "translate(" + x(year) + ",0)")
			.attr("y2", height);
		var bisect = d3.bisector(function(d) { return d.year }).left;
		var i = bisect(data, year);
		var d = data[i];
		
		magic.select('.year > .value').text(d.year);
		var regex = new RegExp(' ', 'g');
		color.domain().forEach(function(ng) {
			magic.select('.c-' + ng.replace(regex, '-') + ' > .count').text(d.counts[ng].toLocaleString());
		});
	})
	.on("click", function() {
		var coords = d3.mouse(this);
		var year = Math.round(x.invert(coords[0]));
		var pathParts = location.pathname.split('/');
		pathParts.pop();
		window.location = pathParts.join('/') + '/' + year + '/instances' + location.search;
	});

var focus = svg.append("g");
focus.append("line")
	.attr("class", "line")
	.style("stroke", "gray")
	.style("stroke-dasharray", "3,3")
	.style("opacity", 0.7)
	.attr("y1", 0)
	.attr("y2", height);

color.domain().forEach(function(name, i) {
	var row = magic.append('tr')
		.attr('class', 'c-' + name.replace(new RegExp(' ', 'g'), '-'))
		.style('color', color.range()[i]);
	row.append('td')
		.style('text-align', 'right')
		.text(name + ':')
	row.append('td')
		.attr('class', 'count')

});

</script>
</body>
</html>
