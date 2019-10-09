/*!
 * Copyright 2019 Hitachi Vantara. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

define([
  'text!./newEdit.html',
  'pentaho/i18n-osgi!hadoopCluster.messages',
  'css!./newEdit.css'
], function (template, i18n) {

  'use strict';

  var options = {
    bindings: {},
    controllerAs: "vm",
    template: template,
    controller: newEditController
  };

  newEditController.$inject = ["$location", "$state", "$q", "$stateParams", "dataService"];

  function newEditController($location, $state, $q, $stateParams, dataService) {
    var vm = this;
    vm.$onInit = onInit;
    vm.onSelectShim = onSelectShim;
    vm.onSelectShimVersion = onSelectShimVersion;
    vm.validateName = validateName;
    vm.getShimVersions = getShimVersions;

    vm.variableImage = "img/variable.svg";

    function onInit() {
      vm.data = $stateParams.data ? $stateParams.data : {};

      vm.header = i18n.get('new.header');
      vm.clusterNameLabel = i18n.get('hadoop.cluster.name.label');
      vm.importLabel = i18n.get('hadoop.cluster.import.label');
      vm.versionLabel = i18n.get('hadoop.cluster.version.label');

      vm.hdfsLabel = i18n.get('new.edit.hdfs.label');
      vm.hostnameLabel = i18n.get('new.edit.hostname.label');
      vm.portLabel = i18n.get('new.edit.port.label');
      vm.usernameLabel = i18n.get('new.edit.username.label');
      vm.passwordLabel = i18n.get('new.edit.password.label');
      vm.jobTrackerLabel = i18n.get('new.edit.job.tracker.label');
      vm.zooKeeperLabel = i18n.get('new.edit.zoo.keeper.label');
      vm.oozieLabel = i18n.get('new.edit.oozie.label');
      vm.kafkaLabel = i18n.get('new.edit.kafka.label');
      vm.bootstrapServerLabel = i18n.get('new.edit.bootstrap.server.label');

      setDialogTitle(i18n.get('hadoop.cluster.title'));

      dataService.getShimIdentifiers().then(function (res) {
        vm.shimVersionJson = res.data;
        var shimNames = [];
        for (var i = 0; i < res.data.length; i++) {
          if (!contains(shimNames, res.data[i].vendor)) {
            shimNames.push(res.data[i].vendor);
          }
        }
        vm.shimNames = shimNames;

        if ($stateParams.data) {
          //TODO: implement edit
          vm.shimName = vm.data.model.shimName;
          vm.shimVersions = getShimVersions(vm.shimName);
          vm.shimVersion = vm.data.model.shimVersion;
        } else {
          vm.data = {
            model: {
              clusterName: "",
              shimName: "",
              shimVersion: "",
              hdfsHostname: "localhost",
              hdfsPort: "8020",
              hdfsUsername: "",
              hdfsPassword: "",
              jobTrackerHostname: "localhost",
              jobTrackerPort: "8032",
              zooKeeperHostname: "localhost",
              zooKeeperPort: "2181",
              oozieHostname: "http://localhost:8080/oozie",
              kafkaBootstrapServers: "",
              type: "new",
              created: false
            }
          };
          vm.shimName = vm.shimNames[0];
        }
      });

      vm.buttons = getButtons();
    }

    function contains(arr, item) {
      for (var i = 0; i < arr.length; i++) {
        if (arr[i] === item) {
          return true;
        }
      }
      return false;
    }

    function onSelectShim(option) {
      vm.data.model.shimName = option;
      vm.shimVersions = getShimVersions(option);
      if (!vm.shimVersion || contains(vm.shimVersions, vm.shimVersion) === false) {
        vm.shimVersion = vm.shimVersions[0];
      }
    }

    function getShimVersions(shimName) {
      var versions = [];
      for (var i = 0; i < vm.shimVersionJson.length; i++) {
        if (vm.shimVersionJson[i].vendor === shimName) {
          versions.push(vm.shimVersionJson[i].version);
        }
      }
      return versions;
    }

    function onSelectShimVersion(option) {
      vm.data.model.shimVersion = option;
    }

    function validateName() {
      //TODO: implement
      return $q(function (resolve) {
        return resolve(true);
      });
    }

    function setDialogTitle(title) {
      try {
        setTitle(title);
      } catch (e) {
        console.log(title);
      }
    }

    function getButtons() {
      return [
        {
          label: i18n.get('controls.next.label'),
          class: "primary",
          isDisabled: function () {
            return !vm.data.model || !vm.data.model.clusterName;
          },
          position: "right",
          onClick: function () {
            validateName().then(function (isValid) {
              if (isValid) {
                $state.go('creating', {data: vm.data, transition: "slideLeft"});
              }
            });
          }
        },
        {
          label: i18n.get('controls.cancel.label'),
          class: "primary",
          position: "right",
          onClick: function () {
            close();
          }
        }];
    }
  }

  return {
    name: "newEdit",
    options: options
  };

});