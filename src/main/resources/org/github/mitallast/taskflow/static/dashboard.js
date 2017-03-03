(function(){
    'strict';

    angular.module('dag', ['ngRoute', 'ngWebSocket', 'ui.ace'])
    .run(function($rootScope){
        $rootScope.aceJson = function(editor){
            var session = editor.getSession();
            session.setTabSize(4);
            session.setUseSoftTabs(true);
            session.setMode('ace/mode/json');

            editor.renderer.setShowGutter(true);
            editor.setShowPrintMargin(false);
        };
    })
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
    .factory('$updates', function($websocket){
        var stream = $websocket('ws://localhost:8080/ws/');
        var subscriptions = {};
        stream.onMessage(function(frame){
            var message = JSON.parse(frame.data);
            console.log("ws event", message);
            var consumers = subscriptions[message.channel];
            if(consumers){
                consumers.forEach(function(subscriber){
                    subscriber(message.event);
                });
            }
        });
        return {
            subscribe: function(channel, consumer){
                console.log("subscribe", channel);
                stream.send(JSON.stringify({ action: 'subscribe', channel: channel }));
                if(!subscriptions[channel]){
                    subscriptions[channel] = [];
                }
                subscriptions[channel].push(consumer);
            },
            unsubscribe: function(channel, consumer){
                if(subscriptions[channel]){
                    subscriptions[channel] = subscriptions[channel]
                    .filter(function(subscriber){
                        return subscriber != consumer;
                    });
                    if(subscriptions[channel].length == 0){
                        stream.send(JSON.stringify({ action: 'unsubscribe', channel: channel }));
                        delete subscriptions[channel];
                    }
                }
            }
        };
    })
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
                $scope.operations = response.data.map(function(item){
                    return item.id;
                });
                $scope.operationsMap = response.data.reduce(function(map, item){
                    map[item.id] = item;
                    return map;
                }, {});
            });
        $scope.addTask = function(){
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
        $scope.validateDag = function(){
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
        $scope.createDag = function(){
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
        $scope.addTask = function(){
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
        $scope.removeTask = function($index){
            $scope.dag.tasks.splice($index, 1);
            $scope.errors = false;
        };
        $scope.operations = [];
        $scope.operationsMap = {};
        $http.get('/api/operation')
            .then(function(response){
                $scope.operations = response.data.map(function(item){
                    return item.id;
                });
                $scope.operationsMap = response.data.reduce(function(map, item){
                    map[item.id] = item;
                    return map;
                }, {});
            });
        $scope.validateDag = function(){
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
        $scope.updateDag = function(){
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
    .controller('DagRunIdCtrl', function($scope, $http, $routeParams, $timeout, $updates){
        $scope.id = $routeParams.id;
        $scope.cancelable = false;
        $scope.cancelDagRun = function(){
            $http.post('/api/dag/run/id/' + $scope.id + '/cancel');
        };
        $scope.isVisible = {};
        $scope.toggleOutput = function(run){
            if($scope.isVisible[run.id]){
                $scope.showOutput(run, false);
            }else{
                $scope.showOutput(run, true);
            }
        };
        $scope.showOutput = function(run, show){
            $scope.isVisible[run.id] = show;
        };
        $scope.updateDagRun = function(dag_run){
            $scope.dag_run = dag_run;
            $scope.cancelable = dag_run.status == "PENDING" || dag_run.status == "RUNNING";
        };
        $scope.updateDagRunStatus = function(event){
            $scope.dag_run.status = event.status;
            $scope.dag_run.startDate = event.startDate;
            $scope.dag_run.finishDate = event.finishDate;
            $scope.cancelable = event.status == "PENDING" || event.status == "RUNNING";
        };
        $scope.updateTaskRunOutput = function(event){
            $scope.dag_run.tasks
            .filter(function(run) { return run.id == event.taskRunId; })
            .forEach(function(run) {
                $scope.showOutput(run, true);
                if(!run.operationResult){
                    run.operationResult = {};
                }
                if(!run.operationResult.output){
                    run.operationResult.output = "";
                }
                run.operationResult.output += event.line;
            });
        };
        $scope.updateTaskRunStatus = function(event){
            $scope.dag_run.tasks
            .filter(function(run) { return run.id == event.taskRunId; })
            .forEach(function(run) {
                if(!run.operationResult){
                    run.operationResult = {};
                }
                if(!run.operationResult.output){
                    run.operationResult.output = "";
                }
                run.startDate = event.startDate;
                run.finishDate = event.finishDate;
                run.status = event.status;
                run.operationResult.status = event.operationStatus;
            });
        };
        $scope.load = function(){
            $http.get('/api/dag/run/id/' + $scope.id)
            .then(function(response){
                $scope.updateDagRun(response.data)
                if($scope.cancelable){
                    $updates.subscribe("dag/run/" + $scope.id, $scope.onUpdate);
                }
            });
        };
        $scope.onUpdate = function(event){
            switch(event.type) {
                case "DagRunUpdated":
                    $scope.updateDagRun(event.dagRun);
                    break;
                case "DagRunStatusUpdated":
                    $scope.updateDagRunStatus(event);
                    break;
                case "TaskRunNewOutputLine":
                    $scope.updateTaskRunOutput(event);
                    break;
                case "TaskRunStatusUpdated":
                    $scope.updateTaskRunStatus(event);
                    break;
            }
        };
        $scope.$on("$destroy", function(){
            $updates.unsubscribe("dag/run/" + $scope.id, $scope.onUpdate);
        });
        $scope.load();
    })
    .controller('DagScheduleCtrl', function($scope, $http){
        $scope.schedules = [];
        $scope.edit = function(schedule){
            schedule.edit = true;
        };
        $scope.update = function(schedule){
            $http.put('/api/dag/schedule', {
                token: schedule.token,
                enabled: schedule.enabled,
                cronExpression: schedule.cronExpression,
            })
            .then(
                function(response){
                    schedule.edit = false;
                    $scope.load();
                },
                function(response){
                    schedule.errors = response.data;
                }
            )
        };
        $scope.enableSchedule = function(schedule){
            $http.put('/api/dag/token/' + schedule.token + '/schedule/enable')
            .then(function(){
                $scope.load();
            })
        };
        $scope.disableSchedule = function(schedule){
            $http.put('/api/dag/token/' + schedule.token + '/schedule/disable')
            .then(function(){
                $scope.load();
            })
        };
        $scope.load = function(){
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
    .directive('diagramGantt', function(){
        return {
            restrict: 'A',
            scope: true,
            transclude: true,
            link: function(scope, element, attrs){
                scope.$watch('dag_run', function(){
                    if(scope.dag_run){
                        scope.render(scope.dag_run);
                    }
                }, true);
                scope.render = function(dag_run){
                    element[0].innerHTML = '';
                    var tasks = dag_run.tasks.filter(function(d){ return d.startDate; })
                    if(tasks.length == 0){
                        return;
                    }

                    var barHeight = 20;
                    var padding = 4;
                    var sidePadding = 20;
                    var gap = barHeight + padding;
                    var bottomPadding = 14;

                    var w = element[0].offsetWidth;
                    var h = gap * tasks.length + padding * 2 + bottomPadding

                    var svg = d3.select(element[0])
                        .append("svg")
                        .attr("width", w)
                        .attr("height", h)
                        .attr("class", "svg");

                    var min = d3.min(tasks, function(d){ return new Date(d.startDate); }).getTime();
                    var max = d3.max(tasks, function(d){ return parseFinishDate(d.finishDate); }).getTime();

                    // in millis
                    var duration = max - min;

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
                    drawTasks(tasks);

                    function parseFinishDate(date){
                        if(date){
                            return new Date(date);
                        }else{
                            return new Date();
                        }
                    }

                    function makeGrid(){
                        var xAxis = d3.axisBottom(timeScale).tickSize(-h+padding, 0, 0)
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

                    function drawTasks(tasks){

                        var rectangles = svg.append('g')
                            .selectAll("rect")
                            .data(tasks)
                            .enter();

                        rectangles.append("rect")
                            .attr("rx", 3)
                            .attr("ry", 3)
                            .attr("x", function(d){
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
                                return d.task.token;
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
    .directive('jsonText', function(){
        return {
            restrict: 'A',
            require: 'ngModel',
            link: function(scope, element, attr, ngModel){
                ngModel.$parsers.push(function(input){
                    try {
                        var json = JSON.parse(input);
                        ngModel.$setValidity('json', true);
                        return json;
                    } catch(e){
                        ngModel.$setValidity('json', false);
                        return undefined;
                    }
                });
                ngModel.$formatters.push(function(data){
                    return JSON.stringify(data);
                });
            }
        };
    });
})();