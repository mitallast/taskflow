(function(){
    'strict';

    angular.module('dag', ['ngRoute'])
    .config(['$routeProvider', '$locationProvider', function($routeProvider, $locationProvider){
        $locationProvider.html5Mode(false);
        $locationProvider.hashPrefix("");
        $routeProvider
            .when('/', {
                redirectTo: '/dag/latest'
            })
            .when('/dag/latest', {
                templateUrl: '/dag-latest.html',
                controller: 'DagLatestCtrl'
            })
            .when('/dag/id/:id', {
                templateUrl: '/dag-state.html',
                controller: 'DagIdCtrl'
            })
            .when('/dag/token/:token', {
                templateUrl: '/dag-state.html',
                controller: 'DagTokenCtrl'
            })
            .when('/dag/create', {
                templateUrl: '/dag-create.html',
                controller: 'DagCreateCtrl'
            })
            .when('/dag/update/id/:id', {
                templateUrl: '/dag-update.html',
                controller: 'DagUpdateCtrl'
            })
            .when('/dag/run', {
                templateUrl: '/dag-run.html',
                controller: 'DagRunCtrl'
            })
            .when('/dag/run/pending', {
                templateUrl: '/dag-run-pending.html',
                controller: 'DagRunPendingCtrl'
            })
            .when('/dag/run/id/:id', {
                templateUrl: '/dag-run-state.html',
                controller: 'DagRunIdCtrl'
            })
            .when('/dag/schedule', {
                templateUrl: '/dag-schedule.html',
                controller: 'DagScheduleCtrl'
            })
            .when('/operations', {
                templateUrl: '/operations.html',
                controller: 'OperationsController'
            })
            .otherwise({
                redirectTo: '/'
            });
    }])
    .controller('SidebarCtrl', function($scope, $location){
        $scope.menus = [
            [
                {href:'dag/latest', title:'Dag latest'},
                {href:'dag/create', title:'Create Dag'},
            ],
            [
                {href:'dag/schedule', title:'Dag schedule'},
            ],
            [
                {href:'dag/run', title:'Dag run'},
                {href:'dag/run/pending', title:'Dag run pending'},
            ],
            [
                {href:'operations', title:'Operations'},
            ]
        ];
        $scope.activeClass = function(page){
            var current = $location.path().substring(1);
            return page === current ? "active" : "";
        };
    })
    .controller('DagLatestCtrl', function($scope, $http){
        $http.get('/api/dag/latest')
        .then(function(response){
            $scope.latest = response.data;
        });
    })
    .controller('DagIdCtrl', function($scope, $http, $routeParams){
        $http.get('/api/dag/id/' + $routeParams.id)
        .then(function(response){
            $scope.dag = response.data;
        });
    })
    .controller('DagTokenCtrl', function($scope, $http, $routeParams){
        $http.get('/api/dag/token/' + $routeParams.token)
        .then(function(response){
            $scope.dag = response.data;
        });
    })
    .controller('DagCreateCtrl', function($scope, $http, $location){
        $scope.dag = {
           token: '',
           tasks: []
        };
        $scope.errors = false;
        $scope.operations = [];
        $scope.operationsMap = {};
        $http.get('/api/operation')
            .then(function(response){
                $scope.operations = response.data.map(function(item) {
                    return item.id;
                });
                $scope.operationsMap = response.data.reduce(function(map, item) {
                    map[item.id] = item;
                    return map;
                }, {});
            });
        $scope.addTask = function() {
            $scope.dag.tasks.push({
                token: '',
                operation: '',
                depends: [],
                command: {
                    config: {},
                    environment: {}
                }
            });
        };
        $scope.validateDag = function() {
            $http.put('/api/dag/validate', $scope.dag)
            .then(
                function(response){
                    $scope.errors = false;
                },
                function(response){
                    $scope.errors = response.data;
                }
            );
        };
        $scope.createDag = function() {
            $http.put('/api/dag', $scope.dag)
            .then(
                function(response){
                    $location.path('/dag/update/id/' + response.data.id)
                },
                function(response){
                    $scope.errors = response.data;
                }
            );
        };
    })
    .controller('DagUpdateCtrl', function($scope, $http, $location, $routeParams){
        $scope.addTask = function() {
            $scope.dag.tasks.push({
                token: '',
                operation: '',
                depends: [],
                command: {
                    config: {},
                    environment: {}
                }
            });
        };
        $scope.operations = [];
        $scope.operationsMap = {};
        $http.get('/api/operation')
            .then(function(response){
                $scope.operations = response.data.map(function(item) {
                    return item.id;
                });
                $scope.operationsMap = response.data.reduce(function(map, item) {
                    map[item.id] = item;
                    return map;
                }, {});
            });
        $scope.validateDag = function() {
            $http.put('/api/dag/validate', $scope.dag)
            .then(
                function(response){
                    $scope.errors = false;
                },
                function(response){
                    $scope.errors = response.data;
                }
            );
        };
        $scope.updateDag = function() {
            $http.post('/api/dag', $scope.dag)
            .then(
                function(response){
                    $location.path('/dag/update/id/' + response.data.id)
                },
                function(response){
                    $scope.errors = response.data;
                }
            );
        };
        $http.get('/api/dag/id/' + $routeParams.id)
        .then(function(response){
            $scope.dag = response.data;
        });
    })
    .controller('CreateDagRunCtrl', function($scope, $http, $location){
        $scope.createDagRunById = function(id){
            $http.put('/api/dag/id/' + id + "/run")
            .then(
                function(response){
                    $location.path('/dag/run/id/' + response.data.id)
                },
                function(response){
                    $scope.errors = response.data;
                }
            )
        };
        $scope.createDagRunByToken = function(token){
            $http.put('/api/dag/token/' + token + "/run")
            .then(
                function(response){
                    $location.path('/dag/run/id/' + response.data.id)
                },
                function(response){
                    $scope.errors = response.data;
                }
            )
        };
    })
    .controller('DagRunCtrl', function($scope, $http){
        $http.get('/api/dag/run')
        .then(function(response){
            $scope.runs = response.data;
        });
    })
    .controller('DagRunPendingCtrl', function($scope, $http){
        $http.get('/api/dag/run/pending')
        .then(function(response){
            $scope.runs = response.data;
        });
    })
    .controller('DagRunIdCtrl', function($scope, $http, $routeParams, $timeout){
        $scope.cancelable = false;
        $scope.cancelDagRun = function() {
            $http.post('/api/dag/run/id/' + $routeParams.id + '/cancel')
            .then(function(response){
                $scope.load();
            });
        };
        $scope.load = function() {
            $http.get('/api/dag/run/id/' + $routeParams.id)
            .then(function(response){
                $scope.dag_run = response.data;
                $scope.cancelable = $scope.dag_run.status == "PENDING" || $scope.dag_run.status == "RUNNING";
                if($scope.cancelable) {
                    $timeout(function(){
                      $scope.load();
                    },100)
                }
            });
        };
        $scope.load();
    })
    .controller('DagScheduleCtrl', function($scope, $http) {
        $scope.schedules = [];
        $scope.edit = function(schedule){
            schedule.edit = true;
        };
        $scope.update = function(schedule){
            console.log(schedule);
            $http.put('/api/dag/schedule', {
                token: schedule.token,
                enabled: schedule.enabled,
                cronExpression: schedule.cronExpression,
            })
            .then(
                function(response) {
                    schedule.edit = false;
                    $scope.load();
                },
                function(response){
                    schedule.errors = response.data;
                }
            )
        };
        $scope.enableSchedule = function(schedule){
            console.log('enableSchedule', schedule)
            $http.put('/api/dag/token/' + schedule.token + '/schedule/enable')
            .then(function(){
                $scope.load();
            })
        };
        $scope.disableSchedule = function(schedule){
            console.log('disableSchedule', schedule)
            $http.put('/api/dag/token/' + schedule.token + '/schedule/disable')
            .then(function(){
                $scope.load();
            })
        };
        $scope.load = function() {
            $http.get('/api/dag/schedule')
            .then(function(response){
                $scope.schedules = response.data;
            });
        }
        $scope.load();
    })
    .controller('OperationsController', function($scope, $http){
        $scope.operations = [];
        $http.get('/api/operation')
        .then(function(response){
            $scope.operations = response.data;
        });
    })
    .directive('diagramGantt', function() {
        return {
            restrict: 'A',
            scope: true,
            transclude: true,
            link: function(scope, element, attrs) {
                scope.$watch('dag_run', function(){
                    if(scope.dag_run){
                        scope.render(scope.dag_run);
                    }
                });
                scope.render = function(dag_run) {
                    element[0].innerHTML = '';
                    var tasks = dag_run.tasks.filter(function(d){ return d.startDate; })
                    if(tasks.length == 0) {
                        return;
                    }

                    var barHeight = 20;
                    var padding = 4;
                    var sidePadding = 20;
                    var gap = barHeight + padding;
                    var bottomPadding = 14;

                    var w = 800;
                    var h = gap * tasks.length + padding * 2 + bottomPadding

                    var svg = d3.select(element[0])
                        .append("svg")
                        .attr("width", w)
                        .attr("height", h)
                        .attr("class", "svg");

                    var min = d3.min(tasks, function(d) { return new Date(d.startDate); }).getTime();
                    var max = d3.max(tasks, function(d) { return parseFinishDate(d.finishDate); }).getTime();

                    // in millis
                    var duration = max - min;
                    console.log(duration);

                    var timeScale = d3.scaleTime()
                        .domain([
                            new Date(min - duration * 0.05),
                            new Date(max + duration * 0.05)
                        ])
                        .range([-2, w+2]);

                    var categories = tasks.map(function(t){ return t.id; })

                    var colorScale = d3.scaleLinear()
                        .domain([0, tasks.length])
                        .range(["#00B9FA", "#F95002"])
                        .interpolate(d3.interpolateHcl);

                    makeGrid();
                    drawRects(tasks);

                    function parseFinishDate(date) {
                        if(date) {
                            return new Date(date);
                        }else{
                            return new Date();
                        }
                    }

                    function makeGrid(){
                        var sec = 1000;
                        var minute = sec * 60;
                        var hour = minute * 60;
                        var day = hour * 24;
                        var week = day * 7;
                        var month = week * 4;
                        var year = month * 4;

                        console.log('minute', minute);
                        console.log('hour', hour);

                        var tickMap = [
                            { duration: year,        ticks: d3.timeMonth.every(2) },
                            { duration: month,       ticks: d3.timeWeek.every(1) },
                            { duration: week,        ticks: d3.timeDay.every(1) },
                            { duration: day,         ticks: d3.timeHour.every(4) },
                            { duration: hour * 12,   ticks: d3.timeHour.every(1) },
                            { duration: hour,        ticks: d3.timeMinute.every(10) },
                            { duration: minute * 30, ticks: d3.timeMinute.every(5) },
                            { duration: minute * 10, ticks: d3.timeMinute.every(1) },
                            { duration: minute,      ticks: d3.timeSecond.every(10) },
                            { duration: sec * 30,    ticks: d3.timeSecond.every(5) },
                            { duration: 0,           ticks: d3.timeSecond.every(1) },
                        ];

                        var ticks = tickMap.filter(function(t){ return t.duration < duration; })[0]
                        console.log(ticks);
                        ticks = ticks.ticks;

                        var xAxis = d3.axisBottom(timeScale)
                            .ticks(ticks)
                            .tickSize(-h+padding, 0, 0)
                            .tickFormat(d3.timeFormat("%X"));

                        var grid = svg.append('g')
                            .attr('class', 'grid')
                            .attr('transform', 'translate(0 , ' + (h - bottomPadding) + ')')
                            .call(xAxis)
                            .selectAll("text")
                                .style("text-anchor", "middle")
                                .attr("fill", "#000")
                                .attr("stroke", "none")
                                .attr("font-size", 10)
                                .attr("dy", "1em");
                    }

                    function drawRects(tasks){

                        var rectangles = svg.append('g')
                            .selectAll("rect")
                            .data(tasks)
                            .enter();

                        rectangles.append("rect")
                            .attr("rx", 3)
                            .attr("ry", 3)
                            .attr("x", function(d) {
                                console.log(new Date(d.startDate))
                                return timeScale(new Date(d.startDate));
                            })
                            .attr("y", function(d, i){ return i*gap + padding; })
                            .attr("width", function(d){ return (timeScale(parseFinishDate(d.finishDate))-timeScale(new Date(d.startDate))); })
                            .attr("height", barHeight)
                            .attr("stroke", "none")
                            .attr("fill", function(d){
                                for (var i = 0; i < categories.length; i++){
                                    if (d.id == categories[i]){
                                        return colorScale(i);
                                    }
                                }
                            })

                        rectangles.append("text")
                            .text(function(d){
                                return d.id;
                            })
                            .attr("x", function(d){
                                return (timeScale(parseFinishDate(d.finishDate))-timeScale(new Date(d.startDate)))/2 + timeScale(new Date(d.startDate));
                            })
                            .attr("y", function(d, i){
                                return i*gap + padding + barHeight / 2 + 4;
                            })
                            .attr("font-size", 11)
                            .attr("text-anchor", "middle")
                            .attr("text-height", barHeight)
                            .attr("fill", "#fff");
                    }
                };
            }
        };
    })
    .directive('jsonText', function() {
        return {
            restrict: 'A',
            require: 'ngModel',
            link: function(scope, element, attr, ngModel) {
                ngModel.$parsers.push(function(input) {
                    try {
                        var json = JSON.parse(input);
                        ngModel.$setValidity('json', true);
                        return json;
                    } catch(e) {
                        ngModel.$setValidity('json', false);
                        return undefined;
                    }
                });
                ngModel.$formatters.push(function(data) {
                    return JSON.stringify(data);
                });
            }
        };
    });
})();